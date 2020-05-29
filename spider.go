package main

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/abadojack/whatlanggo"
	"github.com/gin-gonic/gin"
	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/extensions"
	"github.com/gocolly/colly/v2/storage"
	"github.com/jinzhu/gorm"
	"github.com/qor/admin"
	"github.com/qor/assetfs"
	"github.com/qor/qor/utils"
	log "github.com/sirupsen/logrus"

	"github.com/samirettali/tor-spider/pkg/articletext"
)

/*
	Phobos, Tor66 and Tordex
*/

// Job is a struct that represents a job
type Job struct {
	URL string
}

// PageInfo is a struct used to save the informations about a visited page
type PageInfo struct {
	ID             uint         `gorm:"primary_key" json:"-"`
	CreatedAt      time.Time    `json:"-"`
	UpdatedAt      time.Time    `json:"-"`
	DeletedAt      *time.Time   `sql:"index" json:"-"`
	URL            string       `gorm:"index:url"`
	Body           string       `gorm:"type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longtext"`
	Text           string       `gorm:"type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longtext"`
	Title          string       `gorm:"type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longtext"`
	Domain         string       `gorm:"index:domain"`
	IsHomePage     bool         `gorm:"index:home_page"`
	Status         int          `gorm:"index:status"`
	Language       string       `gorm:"index:language"`
	LangConfidence float64      `json:"-"`
	PageTopic      []*PageTopic `gorm:"many2many:page_topics;" json:"-"`
}

func (p *PageInfo) BeforeCreate() (err error) {
	if p.Text != "" {
		info := whatlanggo.Detect(p.Text)
		p.Language = info.Lang.String()
		p.LangConfidence = info.Confidence
		fmt.Println("======> Language:", p.Language, " Script:", whatlanggo.Scripts[info.Script], " Confidence: ", p.LangConfidence)
	}
	return
}

// PageTopic is a struct used to store topics detected in a visited page (WIP)
type PageTopic struct {
	gorm.Model
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

	rdbms       *gorm.DB
	storage     storage.Storage
	jobsStorage JobsStorage
	pageStorage PageStorage

	Logger *log.Logger
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
	spider.startWebAdmin()

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
	page.Meta(&admin.Meta{
		Name: "Body",
		Type: "text",
	})

	page.Meta(&admin.Meta{
		Name: "Text",
		Type: "text",
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

		result := &PageInfo{
			URL:       r.Request.URL.String(),
			Body:      body,
			Text:      text,
			Status:    r.StatusCode,
			Title:     title,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		var pageExists PageInfo
		if !spider.rdbms.Where("url = ?", r.Request.URL.String()).First(&pageExists).RecordNotFound() {
			spider.Logger.Debugf("skipping link=%s as already exists\n", r.Request.URL.String())
		} else {
			spider.Logger.Debug("Insert into db...")
			err = spider.rdbms.Create(result).Error
			if err != nil {
				spider.Logger.Error(err)
			}
		}

		// index to manticoresearch

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
