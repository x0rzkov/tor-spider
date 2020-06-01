package main

import (
	"crypto/md5"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	// "https://github.com/rsc/pdf"
	"github.com/PuerkitoBio/goquery"
	"github.com/abadojack/whatlanggo"
	"github.com/gin-gonic/gin"
	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/extensions"
	"github.com/gocolly/colly/v2/storage"
	"github.com/jinzhu/gorm"
	"github.com/jpillora/go-tld"
	"github.com/qor/admin"
	"github.com/qor/assetfs"
	"github.com/qor/qor/utils"
	"github.com/qor/validations"
	log "github.com/sirupsen/logrus"
	"github.com/urandom/text-summary/summarize"
	"gopkg.in/jdkato/prose.v2"

	"github.com/samirettali/tor-spider/pkg/articletext"
	"github.com/samirettali/tor-spider/pkg/gowap"
	"github.com/samirettali/tor-spider/pkg/manticore"
)

// Job is a struct that represents a job
type Job struct {
	URL string
}

// PageInfo is a struct used to save the informations about a visited page
type PageInfo struct {
	ID             uint            `gorm:"primary_key" json:"-"`
	CreatedAt      time.Time       `json:"-"`
	UpdatedAt      time.Time       `json:"-"`
	DeletedAt      *time.Time      `sql:"index" json:"-"`
	URL            string          `gorm:"index:url"`
	Body           string          `gorm:"type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longtext"`
	Summary        string          `gorm:"type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longtext"`
	KeyPoints      string          `gorm:"type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longtext"`
	Title          string          `gorm:"type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longtext"`
	Keywords       string          `gorm:"type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longtext"`
	Category       string          `gorm:"index:category"`
	Domain         string          `gorm:"index:domain"`
	IsHomePage     bool            `gorm:"index:home_page"`
	Status         int             `gorm:"index:status"`
	Language       string          `gorm:"index:language"`
	LangConfidence float64         `json:"-"`
	Fingerprint    string          `json:"-" gorm:"index:fingerprint"`
	Wapp           string          `gorm:"type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longtext" json:"-"`
	PageTopic      []*PageTopic    `gorm:"many2many:page_topics;" json:"-"`
	PageProperties PageProperties  `sql:"type:text" json:"-"`
	PageAttributes []PageAttribute `json:"-"`
}

func (p *PageInfo) BeforeCreate() (err error) {
	if p.Summary != "" {
		info := whatlanggo.Detect(p.Summary)
		p.Language = info.Lang.String()
		p.LangConfidence = info.Confidence
		fmt.Println("======> Language:", p.Language, " Script:", whatlanggo.Scripts[info.Script], " Confidence: ", p.LangConfidence)
	}
	return
}

type PageProperties []PageProperty

type PageProperty struct {
	Name  string
	Value string
}

func (pageProperties *PageProperties) Scan(value interface{}) error {
	switch v := value.(type) {
	case []byte:
		return json.Unmarshal(v, pageProperties)
	case string:
		if v != "" {
			return pageProperties.Scan([]byte(v))
		}
	default:
		return errors.New("not supported")
	}
	return nil
}

func (pageProperties PageProperties) Value() (driver.Value, error) {
	if len(pageProperties) == 0 {
		return nil, nil
	}
	return json.Marshal(pageProperties)
}

type PageAttribute struct {
	gorm.Model
	PageInfoID uint
	Name       string
	Value      string
}

func (p PageAttribute) Validate(db *gorm.DB) {
	if strings.TrimSpace(p.Name) == "" {
		db.AddError(validations.NewError(p, "Name", "Name can not be empty"))
	}
	if strings.TrimSpace(p.Value) == "" {
		db.AddError(validations.NewError(p, "Value", "Value can not be empty"))
	}
}

// PageTopic is a struct used to store topics detected in a visited page (WIP)
type PageTopic struct {
	gorm.Model
	PageInfoID     uint
	Name           string `gorm:"index:name"`
	Language       string `gorm:"index:language"`
	LangConfidence float64
}

// Spider is a struct that represents a Spider
type Spider struct {
	numWorkers  int
	parallelism int
	depth       int
	blacklist   []string
	jobs        chan Job
	results     chan PageInfo
	proxyURI    string

	regexTwitter *regexp.Regexp
	regexOnion   *regexp.Regexp
	regexBitcoin *regexp.Regexp
	regexEmail   *regexp.Regexp

	manticore   manticore.Client
	rdbms       *gorm.DB
	storage     storage.Storage
	jobsStorage JobsStorage
	pageStorage PageStorage
	wapp        *gowap.Wappalyzer
	Logger      *log.Logger
}

// JobsStorage is an interface which handles the storage of the jobs when it's
// channel is empty or full.
type JobsStorage interface {
	Init() error
	SaveJob(Job) error
	GetJob() (Job, error)
}

// PageStorage is an interface which handles tha storage of the visited pages
type PageStorage interface {
	Init() error
	SavePage(PageInfo) error
}

// Init initialized all the struct values
func (spider *Spider) Init() error {
	spider.jobs = make(chan Job, spider.numWorkers*spider.parallelism*100)
	spider.results = make(chan PageInfo, 100)
	spider.startWebServer()
	//if spider.admin {
	spider.startWebAdmin()
	//}

	if err := spider.startJobsStorage(); err != nil {
		return err
	}

	if err := spider.pageStorage.Init(); err != nil {
		return err
	}

	return nil
}

func (spider *Spider) startWebAdmin() {
	// Web listener
	addr := ":8889"

	// Initialize AssetFS
	AssetFS := assetfs.AssetFS().NameSpace("admin")

	// Register custom paths to manually saved views
	AssetFS.RegisterPath(filepath.Join(utils.AppRoot, "./templates/qor/admin/views"))
	AssetFS.RegisterPath(filepath.Join(utils.AppRoot, "./templates/qor/media/views"))

	// Initialize Admin
	Admin := admin.New(&admin.AdminConfig{
		SiteName: "Tor Dataset",
		DB:       spider.rdbms,
		AssetFS:  AssetFS,
	})

	// Allow to use Admin to manage Tag, PublicKey, URL, Service
	page := Admin.AddResource(&PageInfo{})
	page.IndexAttrs("ID", "Title", "Language", "URL")

	page.Meta(&admin.Meta{
		Name: "Body",
		Type: "text",
	})

	page.Meta(&admin.Meta{
		Name: "Summary",
		Type: "rich_editor",
	})

	svc := Admin.AddResource(&Service{})
	svc.Meta(&admin.Meta{
		Name: "Description",
		Type: "rich_editor",
	})

	pks := Admin.AddResource(&PublicKey{})
	pks.Meta(&admin.Meta{
		Name: "Value",
		Type: "text",
	})

	Admin.AddResource(&URL{})

	Admin.AddResource(&Tag{})

	// initalize an HTTP request multiplexer
	mux := http.NewServeMux()

	// Mount admin interface to mux
	Admin.MountTo("/admin", mux)

	router := gin.Default()

	// add route to home page
	router.GET("/", func(c *gin.Context) {
		c.String(200, "welcome to your doom.")
	})

	// add route to search page
	router.GET("/search", func(c *gin.Context) {
		c.String(500, "not implemented yet")
	})

	// add route to add new website
	router.GET("/add", func(c *gin.Context) {
		inputUrl, _ := c.GetQuery("url")
		if inputUrl == "" {
			c.String(500, "missing url parameter.")
			return
		}
		spider.Logger.Debugf("Crawling URL: %s", inputUrl)
		go spider.crawl(inputUrl, true)
		c.String(200, "ok")
	})

	// add basic auth
	admin := router.Group("/admin", gin.BasicAuth(gin.Accounts{"tor": "xor"}))
	{
		admin.Any("/*resources", gin.WrapH(mux))
	}

	s := &http.Server{
		Addr:           addr,
		Handler:        router,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	go s.ListenAndServe()

	log.Info("Listening on " + addr)
}

func (spider *Spider) startWebServer() {
	// Web listener
	addr := ":8888"
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		URL := r.URL.Query().Get("url")
		if URL == "" {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Missing url"))
			return
		}
		spider.Logger.Debugf("Crawling URL: %s", URL)
		go spider.crawl(URL, true)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Oki"))
	})
	go http.ListenAndServe(addr, nil)
	log.Info("Listening on " + addr)
}

func (spider *Spider) startJobsStorage() error {
	err := spider.jobsStorage.Init()
	if err != nil {
		return err
	}

	delay := 50 * time.Millisecond
	go func() {
		lowerBound := int(float64(cap(spider.jobs)) * .15)
		for {
			if len(spider.jobs) < lowerBound {
				job, err := spider.jobsStorage.GetJob()
				if err != nil {
					if _, ok := err.(*NoJobsError); ok {
						spider.Logger.Debug("No jobs in storage")
						time.Sleep(delay)
					} else {
						spider.Logger.Error(err)
					}
				} else {
					spider.jobs <- job
					spider.Logger.Debugf("Got Job %v", job)
				}
			} else {
				time.Sleep(delay)
			}
		}
	}()

	go func() {
		upperBound := int(float64(cap(spider.jobs)) * .85)
		for {
			if len(spider.jobs) > upperBound {
				job := <-spider.jobs
				err := spider.jobsStorage.SaveJob(job)
				if err != nil {
					log.Error(err)
				}
			} else {
				time.Sleep(delay)
			}
		}
	}()
	return nil
}

func (spider *Spider) getCollector() (*colly.Collector, error) {
	disallowed := make([]*regexp.Regexp, len(spider.blacklist))
	for index, b := range spider.blacklist {
		disallowed[index] = regexp.MustCompile(b)
	}
	c := colly.NewCollector(
		// colly.AllowURLRevisit(),
		colly.MaxDepth(spider.depth),
		colly.Async(true),
		colly.IgnoreRobotsTxt(),
		// colly.Debugger(&debug.LogDebugger{}),
		colly.DisallowedURLFilters(
			disallowed...,
		),
		colly.URLFilters(
			regexp.MustCompile(`http://[a-zA-Z2-7]{16}\.onion.*`),
			regexp.MustCompile(`http://[a-zA-Z2-7]{56}\.onion.*`),
			regexp.MustCompile(`https://[a-zA-Z2-7]{16}\.onion.*`),
			regexp.MustCompile(`https://[a-zA-Z2-7]{56}\.onion.*`),
		),
	)

	c.MaxBodySize = 1000 * 1000

	extensions.RandomUserAgent(c)
	extensions.Referer(c)

	proxyURL, err := url.Parse(spider.proxyURI)
	if err != nil {
		return nil, err
	}

	c.WithTransport(&http.Transport{
		Proxy: http.ProxyURL(proxyURL),
		DialContext: (&net.Dialer{
			Timeout:   60 * time.Second,
			KeepAlive: 60 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   60 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableKeepAlives:     true,
	})

	c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: spider.parallelism,
	})

	if err := c.SetStorage(spider.storage); err != nil {
		return nil, err
	}

	// Get all the links
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		foundURL := e.Request.AbsoluteURL(e.Attr("href"))
		if foundURL != "" && e.Request.Depth == spider.depth {
			spider.jobs <- Job{foundURL}
		} else {
			e.Request.Visit(foundURL)
		}
	})

	// Save result
	c.OnResponse(func(r *colly.Response) {
		body := string(r.Body)
		bodyReader := strings.NewReader(body)

		dom, err := goquery.NewDocumentFromReader(bodyReader)
		title := ""
		if err != nil {
			spider.Logger.Error(err)
			return
		} else {
			title = dom.Find("title").Contents().Text()
		}

		if title == "" {
			spider.Logger.Error(errors.New("not an html page"))
			return
		}

		text, err := articletext.GetArticleTextFromDocument(dom)
		if err != nil {
			spider.Logger.Error(err)
		}

		// extract the domain vanity hash
		u, _ := tld.Parse(r.Request.URL.String())
		spider.Logger.Debugf("[parseDomain] subdomain=%s, domain=%s", u.Subdomain, u.Domain)

		// extract a md5 hash of the text to avoid duplicate content (login pages, captachas,...)
		fingerprint := strToMD5(text)

		// check if home page
		home, err := url.Parse(r.Request.URL.String())
		if err != nil {
			spider.Logger.Error(err)
		}

		// extract key points
		s := summarize.NewFromString(title, text)
		keyPoints := s.KeyPoints()

		result := &PageInfo{
			URL:         r.Request.URL.String(),
			Summary:     text,
			KeyPoints:   strings.Join(keyPoints, "|"),
			Domain:      u.Domain,
			Status:      r.StatusCode,
			Title:       title,
			Fingerprint: fingerprint,
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		}

		// var isHomePage bool
		if home.RequestURI() == "" || home.RequestURI() == "/" {
			result.IsHomePage = true
			// gowap the tor-website
			res, err := spider.wapp.Analyze(r.Request.URL.String())
			if err != nil {
				spider.Logger.Error(err)
			}
			// prettyJSON, err := json.MarshalIndent(res, "", "  ")
			wappJson, err := json.Marshal(res)
			if err != nil {
				spider.Logger.Error(err)
			}
			result.Wapp = string(wappJson)
		}

		// check for bitcoins and email addresses
		emails := spider.regexEmail.FindAllString(body, -1)
		emails = removeDuplicates(emails)
		for _, email := range emails {
			result.PageAttributes = append(result.PageAttributes, PageAttribute{Name: "email", Value: email})
			result.PageProperties = append(result.PageProperties, PageProperty{Name: "email", Value: email})
		}
		bitcoins := spider.regexBitcoin.FindAllString(body, -1)
		bitcoins = removeDuplicates(bitcoins)
		for _, bitcoin := range bitcoins {
			result.PageAttributes = append(result.PageAttributes, PageAttribute{Name: "bitcoin", Value: bitcoin})
			result.PageProperties = append(result.PageProperties, PageProperty{Name: "bitcoin", Value: bitcoin})
		}

		twitters := spider.regexTwitter.FindAllString(body, -1)
		twitters = removeDuplicates(twitters)
		for _, twitter := range twitters {
			result.PageAttributes = append(result.PageAttributes, PageAttribute{Name: "twitter", Value: twitter})
			result.PageProperties = append(result.PageProperties, PageProperty{Name: "twitter", Value: twitter})
		}

		/*
			onions := spider.regexOnion.FindAllString(body, -1)
			for _, onion := range onions {
				result.PageAttributes = append(result.PageAttributes, PageAttribute{Name: "outbound", Value: onion})
				result.PageProperties = append(result.PageProperties, PageProperty{Name: "outbound", Value: onion})
				spider.jobs <- Job{onion}
			}
		*/

		// keywords
		var topicsProse []string
		doc, _ := prose.NewDocument(text)
		for _, ent := range doc.Entities() {
			spider.Logger.Debugf("[entity] ent.Text=%s, ent.Label=%s", ent.Text, ent.Label)
			topic := ent.Text
			if len(topic) > 16 {
				continue
			}
			if topic != "" {
				topicsProse = append(topicsProse, topic)
			}
		}
		topicsProse = removeDuplicates(topicsProse)
		result.Keywords = strings.Join(topicsProse, ",")

		var pageExists PageInfo
		if !spider.rdbms.Where("fingerprint = ?", fingerprint).First(&pageExists).RecordNotFound() {
			spider.Logger.Debugf("skipping link=%s as similar content already exists\n", r.Request.URL.String())
			// if simalar content exists, skip from mysql and elasticsearch indexation
			return
		} else {
			spider.Logger.Debug("Insert into db...")
			err = spider.rdbms.Create(result).Error
			if err != nil {
				spider.Logger.Error(err)
				return
			}
		}

		// index to manticoresearch
		// how to cope with the new manticore json api ?!
		// curl -X POST 'http://127.0.0.1:9308/json/insert' -d'{"index":"testrt","id":1,"doc":{"title":"Hello","content":"world","gid":1}}'

		// index to elasticsearch
		err = spider.pageStorage.SavePage(*result)
		if err != nil {
			spider.Logger.Error(err)
		}

	})

	// Debug responses
	c.OnResponse(func(r *colly.Response) {
		spider.Logger.Debugf("Got %d for %s", r.StatusCode,
			r.Request.URL)
	})

	// Debug errors
	c.OnError(func(r *colly.Response, err error) {
		spider.Logger.Debugf("Error while visiting %s: %v", r.Request.URL, err)
	})

	return c, nil
}

func (spider *Spider) getInputCollector() (*colly.Collector, error) {
	c := colly.NewCollector(
		colly.MaxDepth(3),
		colly.Async(true),
		colly.IgnoreRobotsTxt(),
	)

	c.MaxBodySize = 1000 * 1000

	extensions.RandomUserAgent(c)
	extensions.Referer(c)

	proxyURL, err := url.Parse(spider.proxyURI)
	if err != nil {
		return nil, err
	}

	c.WithTransport(&http.Transport{
		Proxy: http.ProxyURL(proxyURL),
		DialContext: (&net.Dialer{
			Timeout: 60 * time.Second,
			// KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   60 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableKeepAlives:     true,
	})

	c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: spider.parallelism,
	})

	// Get all the links
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		foundURL := e.Request.AbsoluteURL(e.Attr("href"))
		spider.jobs <- Job{foundURL}
		e.Request.Visit(foundURL)
	})

	// Debug responses
	c.OnResponse(func(r *colly.Response) {
		spider.Logger.Debugf("InputCollector got %d for %s", r.StatusCode,
			r.Request.URL)
	})

	// Debug errors
	c.OnError(func(r *colly.Response, err error) {
		spider.Logger.Debugf("InputCollector error for %s: %s", r.Request.URL,
			err)
	})

	return c, nil
}

// Start starts the crawlers and logs messages
func (spider *Spider) Start() {
	sem := make(chan int, spider.numWorkers)
	go func() {
		for {
			job := <-spider.jobs
			sem <- 1
			go func(seed string) {
				spider.crawl(seed, false)
				<-sem
			}(job.URL)
		}
	}()

	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		spider.Logger.Infof("There are %d jobs and %d collectors running", len(spider.jobs), len(sem))
	}
}

func (spider *Spider) crawl(seed string, input bool) {
	var c *colly.Collector
	var err error
	spider.Logger.Debugf("seed=%s, input=%t", seed, input)

	if input {
		c, err = spider.getInputCollector()
	} else {
		c, err = spider.getCollector()
	}

	if err != nil {
		spider.Logger.Error(err)
		return
	}

	err = c.Visit(seed)
	if err == nil {
		spider.Logger.Debugf("Collector started on %s", seed)
	} else {
		spider.Logger.Debugf("Collector could not start, err %s", err)
	}
	c.Wait()
}

func removeDuplicates(elements []string) []string {
	// Use map to record duplicates as we find them.
	encountered := map[string]bool{}
	result := []string{}

	for v := range elements {
		if encountered[elements[v]] == true {
			// Do not add duplicate.
		} else {
			// Record this element as an encountered element.
			encountered[elements[v]] = true
			// Append to result slice.
			result = append(result, elements[v])
		}
	}
	// Return the new slice.
	return result
}

func strToMD5(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}
