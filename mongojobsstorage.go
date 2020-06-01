package main

import (
	"context"
	"errors"
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoJobsStorage is an implementation of the JobsStorage interface
type MongoJobsStorage struct {
	DatabaseName   string
	CollectionName string
	Logger         *log.Logger
	URI            string
	jobs           chan Job
	collection     *mongo.Collection
}

// Init initializes the collection
func (s *MongoJobsStorage) Init() error {
	if s.collection == nil {
		s.jobs = make(chan Job, 100)
		var client *mongo.Client
		var err error
		if client, err = mongo.NewClient(options.Client().ApplyURI(s.URI)); err != nil {
			log.Warnln("mongo.NewClient", err)
			return err
		}
		if err = client.Connect(context.Background()); err != nil {
			log.Warnln("client.Connect", err)
			return err
		}
		db := client.Database(s.DatabaseName)
		s.collection = db.Collection(s.CollectionName)
	}
	return nil
}

// GetJob returns a job
func (s *MongoJobsStorage) GetJob() (Job, error) {
	var job Job
	ctx := context.Background()

	// ref
	// https://github.com/St0iK/go-quote-bot/blob/master/dao/quotes_dao.go
	rand.Seed(time.Now().UnixNano())

	/*
		// count documents
		c, _ := collection.CountDocuments(ctx, bson.D{}, nil)

		// get random doc
		random := rand.Int63n(c-1) + 1
		findOptions := options.Find()
		findOptions.SetLimit(1)
		findOptions.SetSkip(random)

		cur, err := s.collection.Find(ctx, bson.D{}, findOptions)
		if err != nil {
			log.Fatal(err)
		}
	*/

	err := s.collection.FindOne(ctx, bson.D{}).Decode(&job)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return job, &NoJobsError{err.Error()}
		}
		return job, err
	}
	_, err = s.collection.DeleteOne(ctx, job)
	if err != nil {
		return job, err
	}
	return job, nil
}

// SaveJob adds a job to the jobs channel, upon checking if it's full
func (s *MongoJobsStorage) SaveJob(job Job) error {
	select {
	case s.jobs <- job:
		return nil
	default:
		err := s.flush(len(s.jobs))
		if err != nil {
			return err
		}
		s.jobs <- job
		return nil
	}
}

func (s *MongoJobsStorage) flush(quantity int) error {
	ctx := context.Background()
	jobs := make([]interface{}, 0)
	for i := 0; i < quantity; i++ {
		job := <-s.jobs
		jobs = append(jobs, job)
	}
	_, err := s.collection.InsertMany(ctx, jobs)
	s.Logger.Debugf("Saved %d jobs", quantity)
	return err
}
