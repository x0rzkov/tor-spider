package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/gocolly/redisstorage"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/jpillora/go-tld"
	"github.com/k0kubun/pp"
	_ "github.com/mattn/go-sqlite3"
	"github.com/qor/media"
	"github.com/qor/validations"
	log "github.com/sirupsen/logrus"
	ccsv "github.com/tsak/concurrent-csv-writer"

	"github.com/samirettali/tor-spider/pkg/gowap"
	"github.com/samirettali/tor-spider/pkg/manticore"
)

func main() {
	blacklistFile := flag.String("b", "", "blacklist file")
	depth := flag.Int("d", 2, "depth of each collector")
	verbose := flag.Bool("v", false, "verbose")
	debug := flag.Bool("x", false, "debug")
	numWorkers := flag.Int("w", 12, "number of workers")
	parallelism := flag.Int("p", 32, "parallelism of workers")
	oniontree := flag.Bool("o", false, "import oniontree")
	dumpUrls := flag.Bool("u", false, "dump urls from oniontree")
	fixDomain := flag.Bool("f", false, "fix missing domains")
	// isAdmin := flag.Bool("a", false, "start webui admin")
	indexManticore := flag.Bool("i", false, "index to manticore")
	searchManticore := flag.String("s", "", "search manticore index")

	flag.Parse()

	logger := log.New()
	// logger.SetReportCaller(true)
	logger.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})

	if *debug {
		logger.SetLevel(log.DebugLevel)
	} else if *verbose {
		logger.SetLevel(log.InfoLevel)
	}

	// Setting up RDBMS
	db, err := gorm.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8mb4,utf8&parseTime=True", os.Getenv("TOR_MYSQL_USER"), os.Getenv("TOR_MYSQL_PASSWORD"), os.Getenv("TOR_MYSQL_HOST"), os.Getenv("TOR_MYSQL_PORT"), os.Getenv("TOR_MYSQL_DATABASE")))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// callback for images and validation
	validations.RegisterCallbacks(db)
	media.RegisterCallbacks(db)

	// migrate tables
	db.AutoMigrate(&PageInfo{})
	db.AutoMigrate(&PageTopic{})
	db.AutoMigrate(&PageAttribute{})
	db.AutoMigrate(&Tag{})
	db.AutoMigrate(&Service{})
	db.AutoMigrate(&URL{})
	db.AutoMigrate(&PublicKey{})

	if *fixDomain {
		var pages []PageInfo
		db.Where("domain = ?", "").Find(&pages)
		for _, page := range pages {
			u, _ := tld.Parse(page.URL)
			page.Domain = u.Domain
			log.Info("domain: ", u.Domain)
			err := db.Save(page).Error
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	if *dumpUrls {
		csvDataset, err := ccsv.NewCsvWriter("urls.txt")
		if err != nil {
			panic("Could not open `dataset.txt` for writing")
		}

		// Flush pending writes and close file upon exit of Sitemap()
		defer csvDataset.Close()

		type res struct {
			Name string
		}

		var results []res
		db.Raw("select name FROM urls").Scan(&results)
		for _, result := range results {
			csvDataset.Write([]string{result.Name})
			csvDataset.Flush()
		}
		os.Exit(1)
	}

	// Mantincore for indexing content
	cl, _, err := initSphinx("127.0.0.1", 9312)
	checkErr(err)

	if *searchManticore != "" {
		fmt.Println("query:", *searchManticore)
		res2, err2 := cl.Query(*searchManticore, "rt_tor_spider")
		// curl -X POST 'http://127.0.0.1:9308/sql' -d 'mode=raw&query=CREATE TABLE testrt ( title text, content text, gid integer)'
		// curl -X POST 'http://127.0.0.1:9308/json/insert' -d'{"index":"rt_tor_spider","id":1,"doc":{"title":"Hello","content":"world","gid":1}}' | jq .
		// curl -X POST 'http://127.0.0.1:9308/json/search' -d '{"index":"rt_tor_spider","query":{"match":{"*":"dataset"}}}' | jq .
		pp.Println(res2, err2)
		os.Exit(1)
	}

	if *indexManticore {

		cl, err := sql.Open("mysql", "@tcp(127.0.0.1:9306)/")
		if err != nil {
			panic(err)
		}

		var pageInfos []*PageInfo
		db.Where("status = ?", "200").Find(&pageInfos)
		for _, pageInfo := range pageInfos {
			var deletedAt time.Time
			if pageInfo.DeletedAt == nil {
				deletedAt = time.Date(2001, time.January, 01, 01, 0, 0, 0, time.UTC)
			} else {
				deletedAt = *pageInfo.DeletedAt
			}

			query := fmt.Sprintf(`REPLACE into rt_tor_spider (id,created_at,updated_at,deleted_at,url,summary,title,is_home_page,status,language,domain,category,wapp,page_properties) VALUES ('%d','%d','%d','%d','%s','%s','%s','%t','%d','%s','%s','%s','%s','%s')`,
				pageInfo.ID,
				pageInfo.CreatedAt.Unix(),
				pageInfo.UpdatedAt.Unix(),
				deletedAt.Unix(),
				escape(pageInfo.URL),
				escape(pageInfo.Summary),
				escape(pageInfo.Title),
				pageInfo.IsHomePage,
				pageInfo.Status,
				pageInfo.Language,
				pageInfo.Domain,
				pageInfo.Category,
				pageInfo.Wapp,
				pageInfo.PageProperties,
			)
			//fmt.Println(query)
			_, err := cl.Exec(query)
			if err != nil {
				log.Warnln("index err", err)
			}
			// pp.Println(res)
		}
		os.Exit(1)
	}

	// Setting up storage
	// Redis for visited pages
	redisURI, ok := os.LookupEnv("REDIS_URI")
	if !ok {
		log.Fatal("You must set REDIS_URI env variable")
	}

	fmt.Println("REDIS_URI", redisURI)

	visitedStorage := &redisstorage.Storage{
		Address:  redisURI,
		Password: "",
		DB:       0,
		Prefix:   "0",
	}
	// defer visitedStorage.Client.Close()

	// Elastic for page saving
	elasticURI, ok := os.LookupEnv("ELASTIC_URI")
	if !ok {
		logger.Error("You must set ELASTIC_URI env variable")
	}
	elasticIndex, ok := os.LookupEnv("ELASTIC_INDEX")
	if !ok {
		logger.Error("You must set ELASTIC_INDEX env variable")
	}
	pageStorage := &ElasticPageStorage{
		URI:        elasticURI,
		Index:      elasticIndex,
		BufferSize: 100,
		Logger:     logger,
	}

	// Mongo for jobs storage
	mongoURI, ok := os.LookupEnv("MONGO_URI")
	if !ok {
		logger.Error("You must define MONGO_URI env variable")
	}
	mongoDB, ok := os.LookupEnv("MONGO_DB")
	if !ok {
		logger.Error("You must set MONGO_DB env variable")
	}
	mongoCol, ok := os.LookupEnv("MONGO_COL")
	if !ok {
		logger.Error("You must set MONGO_COL env variable")
	}
	jobsStorage := &MongoJobsStorage{
		URI:            mongoURI,
		DatabaseName:   mongoDB,
		CollectionName: mongoCol,
		Logger:         logger,
	}
	pp.Println("jobsStorage", jobsStorage)

	proxyURI, ok := os.LookupEnv("PROXY_URI")
	if !ok {
		logger.Error("You must set PROXY_URI env variable")
	}

	// instanciate gowap dictionary
	wapp, err := gowap.Init("./shared/dataset/wappalyzer/apps.json", proxyURI, false)
	if err != nil {
		log.Fatal(err)
	}

	bitcoinPatternRegexp, err := regexp.Compile(`[13][a-km-zA-HJ-NP-Z0-9]{26,33}$`)
	if err != nil {
		log.Fatal(err)
	}

	emailPatternRegexp, err := regexp.Compile(`([a-zA-Z0-9_\-\.]+)@([a-zA-Z0-9_\-\.]+)\.([a-zA-Z]{2,5})$`)
	if err != nil {
		log.Fatal(err)
	}

	// (?:https?://)?(?:www)?(\S*?\.onion)\b
	onionPatternRegexp, err := regexp.Compile(`(?:https?\:\/\/)?[\w\-\.]+\.onion`)
	if err != nil {
		log.Fatal(err)
	}

	twitterPatternRegexp, err := regexp.Compile(`(https?\:)?(//)(www[\.])?(twitter.com/)([a-zA-Z0-9_]{1,15})[\/]?`)
	if err != nil {
		log.Fatal(err)
	}

	/*
		// 2018-01-04T05:52:20.698
		// 2018-01-04T05:52:34
		dateTime, err := regexp.Compile(`\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d(?:\.\d+)?Z?`)
		if err != nil {
			log.Fatal(err)
		}

		// 2015-1-11 13:57:24
		// ^((0?[13578]|10|12)(-|\/)(([1-9])|(0[1-9])|([12])([0-9]?)|(3[01]?))(-|\/)((19)([2-9])(\d{1})|(20)([01])(\d{1})|([8901])(\d{1}))|(0?[2469]|11)(-|\/)(([1-9])|(0[1-9])|([12])([0-9]?)|(3[0]?))(-|\/)((19)([2-9])(\d{1})|(20)([01])(\d{1})|([8901])(\d{1})))$
	*/

	spider := &Spider{
		rdbms:        db,
		manticore:    cl,
		storage:      visitedStorage,
		jobsStorage:  jobsStorage,
		pageStorage:  pageStorage,
		proxyURI:     proxyURI,
		numWorkers:   *numWorkers,
		parallelism:  *parallelism,
		depth:        *depth,
		wapp:         wapp,
		regexTwitter: twitterPatternRegexp,
		regexBitcoin: bitcoinPatternRegexp,
		regexEmail:   emailPatternRegexp,
		regexOnion:   onionPatternRegexp,
		Logger:       logger,
	}

	pp.Println(spider)

	if *blacklistFile != "" {
		blacklist, err := readLines(*blacklistFile)
		if err != nil {
			log.Fatal("Error while reading " + *blacklistFile)
		}
		spider.blacklist = blacklist
	}

	if *oniontree {
		spider.importOnionTree("./shared/dataset/oniontree/tagged")
	}

	err = spider.Init()
	if err != nil {
		log.Fatalf("Spider ended with %v", err)
	}

	spider.Start()
}

func checkErr(err error) {
	if err != nil {
		log.Fatalf("error: %v", err)
	}
}

func initSphinx(host string, port uint16) (manticore.Client, bool, error) {
	cl := manticore.NewClient()
	cl.SetServer(host, port)
	status, err := cl.Open()
	if err != nil {
		return cl, status, err
	}
	return cl, status, nil
}

func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func escape(sql string) string {
	dest := make([]byte, 0, 2*len(sql))
	var escape byte
	for i := 0; i < len(sql); i++ {
		c := sql[i]

		escape = 0

		switch c {
		case 0: /* Must be escaped for 'mysql' */
			escape = '0'
			break
		case '\n': /* Must be escaped for logs */
			escape = 'n'
			break
		case '\r':
			escape = 'r'
			break
		case '\\':
			escape = '\\'
			break
		case '\'':
			escape = '\''
			break
		case '"': /* Better safe than sorry */
			escape = '"'
			break
		case '\032': /* This gives problems on Win32 */
			escape = 'Z'
		}

		if escape != 0 {
			dest = append(dest, '\\', escape)
		} else {
			dest = append(dest, c)
		}
	}

	return string(dest)
}
