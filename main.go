package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"

	"github.com/gocolly/redisstorage"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/k0kubun/pp"
	_ "github.com/mattn/go-sqlite3"
	"github.com/qor/media"
	"github.com/qor/validations"
	log "github.com/sirupsen/logrus"
)

func main() {
	blacklistFile := flag.String("b", "", "blacklist file")
	depth := flag.Int("d", 10, "depth of each collector")
	verbose := flag.Bool("v", false, "verbose")
	debug := flag.Bool("x", false, "debug")
	numWorkers := flag.Int("w", 32, "number of workers")
	parallelism := flag.Int("p", 4, "parallelism of workers")
	oniontree := flag.Bool("o", false, "import oniontree")

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
	db.AutoMigrate(&Tag{})
	db.AutoMigrate(&Service{})
	db.AutoMigrate(&URL{})
	db.AutoMigrate(&PublicKey{})

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

	spider := &Spider{
		rdbms:       db,
		storage:     visitedStorage,
		jobsStorage: jobsStorage,
		pageStorage: pageStorage,
		proxyURI:    proxyURI,
		numWorkers:  *numWorkers,
		parallelism: *parallelism,
		depth:       *depth,
		Logger:      logger,
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
