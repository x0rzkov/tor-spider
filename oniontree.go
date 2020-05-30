package main

import (
	"fmt"
	stdioutil "io/ioutil"
	"os"
	"strings"

	"github.com/goccy/go-yaml"
	"github.com/gosimple/slug"
	"github.com/jinzhu/gorm"
	"github.com/k0kubun/pp"
	"github.com/karrick/godirwalk"
	log "github.com/sirupsen/logrus"

	"github.com/onionltd/oniontree-tools/pkg/types/service"
)

var (
	// db     *gorm.DB
	tables = []interface{}{
		&Tag{},
		&Service{},
		&PublicKey{},
		&URL{},
	}
)

// Create a GORM-backend model
type Tag struct {
	gorm.Model
	Name string `gorm:"size:64;unique" json:"name" yaml:"name"`
}

type Service struct {
	gorm.Model
	Name        string       `json:"name" yaml:"name"`
	Slug        string       `json:"slug,omitempty" yaml:"slug,omitempty"`
	Description string       `gorm:"type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longtext" json:"description,omitempty" yaml:"description,omitempty"`
	URLs        []*URL       `json:"urls,omitempty" yaml:"urls,omitempty"`
	PublicKeys  []*PublicKey `json:"public_keys,omitempty" yaml:"public_keys,omitempty"`
	Tags        []*Tag       `gorm:"many2many:service_tags;" json:"tags,omitempty" yaml:"tags,omitempty"`
	Wapp        string       `gorm:"type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longtext"`
}

type URL struct {
	gorm.Model
	Name      string `gorm:"size:255;unique" json:"href" yaml:"href"`
	Healthy   bool   `json:"healthy" yaml:"healthy"`
	ServiceID uint   `json:"-" yaml:"-"`
}

type PublicKey struct {
	gorm.Model
	UID         string `gorm:"primary_key" json:"id,omitempty" yaml:"id,omitempty"`
	UserID      string `json:"user_id,omitempty" yaml:"user_id,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty" yaml:"fingerprint,omitempty"`
	Description string `gorm:"type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longtext" json:"description,omitempty" yaml:"description,omitempty"`
	Value       string `gorm:"type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longtext" json:"value" yaml:"value"`
	ServiceID   uint   `json:"-" yaml:"-"`
}

func (spider *Spider) importOnionTree(dirname string) {
	err := godirwalk.Walk(dirname, &godirwalk.Options{
		Callback: func(osPathname string, de *godirwalk.Dirent) error {
			if !de.IsDir() {
				parts := strings.Split(osPathname, "/")
				// fmt.Printf("Type:%s osPathname:%s tag:%s\n", de.ModeType(), osPathname, parts[1])
				spider.Logger.Infof("Type:%s osPathname:%s tag:%s\n", de.ModeType(), osPathname, parts[1])

				bytes, err := stdioutil.ReadFile(osPathname)
				if err != nil {
					return err
				}
				t := service.Service{}
				yaml.Unmarshal(bytes, &t)
				pp.Println(t)

				// add service
				m := &Service{
					Name:        t.Name,
					Description: t.Description,
					Slug:        slug.Make(t.Name),
				}

				if err := spider.rdbms.Create(m).Error; err != nil {
					fmt.Println(err)
					os.Exit(1)
				}

				// add public keys
				for _, publicKey := range t.PublicKeys {
					pubKey := &PublicKey{
						UID:         publicKey.ID,
						UserID:      publicKey.UserID,
						Fingerprint: publicKey.Fingerprint,
						Description: publicKey.Description,
						Value:       publicKey.Value,
					}
					if _, err := createOrUpdatePublicKey(spider.rdbms, m, pubKey); err != nil {
						fmt.Println(err)
						os.Exit(1)
					}
				}

				// add urls
				for _, url := range t.URLs {
					var urlExists URL
					u := &URL{Name: url}
					if spider.rdbms.Where("name = ?", url).First(&urlExists).RecordNotFound() {
						spider.rdbms.Create(&u)
						pp.Println(u)
					}
					if _, err := createOrUpdateURL(spider.rdbms, m, u); err != nil {
						fmt.Println(err)
						os.Exit(1)
					}

				}

				// add tags
				// check if tag already exists
				tag := &Tag{Name: parts[4]}
				var tagExists Tag
				if spider.rdbms.Where("name = ?", parts[1]).First(&tagExists).RecordNotFound() {
					spider.rdbms.Create(&tag)
					pp.Println(tag)
				}

				if _, err := createOrUpdateTag(spider.rdbms, m, tag); err != nil {
					fmt.Println(err)
					os.Exit(1)
				}

			}
			return nil
		},
		Unsorted: true, // (optional) set true for faster yet non-deterministic enumeration (see godoc)
	})
	if err != nil {
		log.Fatal(err)
	}

}

func findPublicKeyByUID(db *gorm.DB, uid string) *PublicKey {
	pubKey := &PublicKey{}
	if err := db.Where(&PublicKey{UID: uid}).First(pubKey).Error; err != nil {
		log.Fatalf("can't find public_key with uid = %q, got err %v", uid, err)
	}
	return pubKey
}

func createOrUpdateTag(db *gorm.DB, svc *Service, tag *Tag) (bool, error) {
	var existingSvc Service
	if db.Where("slug = ?", svc.Slug).First(&existingSvc).RecordNotFound() {
		err := db.Create(svc).Error
		return err == nil, err
	}
	var existingTag Tag
	if db.Where("name = ?", tag.Name).First(&existingTag).RecordNotFound() {
		err := db.Create(tag).Error
		return err == nil, err
	}
	svc.ID = existingSvc.ID
	svc.CreatedAt = existingSvc.CreatedAt
	svc.Tags = append(svc.Tags, &existingTag)
	return false, db.Save(svc).Error
}

func createOrUpdatePublicKey(db *gorm.DB, svc *Service, pubKey *PublicKey) (bool, error) {
	var existingSvc Service
	if db.Where("slug = ?", svc.Slug).First(&existingSvc).RecordNotFound() {
		err := db.Create(svc).Error
		return err == nil, err
	}
	var existingPublicKey PublicKey
	if db.Where("uid = ?", pubKey.UID).First(&existingPublicKey).RecordNotFound() {
		err := db.Create(pubKey).Error
		return err == nil, err
	}
	svc.ID = existingSvc.ID
	svc.CreatedAt = existingSvc.CreatedAt
	svc.PublicKeys = append(svc.PublicKeys, &existingPublicKey)
	return false, db.Save(svc).Error
}

func createOrUpdateURL(db *gorm.DB, svc *Service, url *URL) (bool, error) {
	var existingSvc Service
	if db.Where("slug = ?", svc.Slug).First(&existingSvc).RecordNotFound() {
		err := db.Create(svc).Error
		return err == nil, err
	}
	var existingURL URL
	if db.Where("name = ?", url.Name).First(&existingURL).RecordNotFound() {
		err := db.Create(url).Error
		return err == nil, err
	}
	svc.ID = existingSvc.ID
	svc.CreatedAt = existingSvc.CreatedAt
	svc.URLs = append(svc.URLs, &existingURL)
	return false, db.Save(svc).Error
}
