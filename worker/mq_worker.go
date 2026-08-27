package worker

import (
	"encoding/json"
	"fmt"
	"github.com/sa7mon/s3scanner/bucket"
	"github.com/sa7mon/s3scanner/db"
	"github.com/sa7mon/s3scanner/mq"
	"github.com/sa7mon/s3scanner/provider"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"os"
	"sync"
)

func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// MQConfig groups the shared configuration used by every WorkMQ consumer.
type MQConfig struct {
	Conn        *amqp.Connection
	Provider    provider.StorageProvider
	Queue       string
	Threads     int
	DoEnumerate bool
	WriteToDB   bool
}

func WorkMQ(threadID int, wg *sync.WaitGroup, cfg MQConfig) {
	_, once := os.LookupEnv("TEST_MQ") // If we're being tested, exit after one bucket is scanned
	defer wg.Done()

	// Wrap the whole thing in a for (while) loop so if the mq server kills the channel, we start it up again
	for {
		ch, chErr := mq.Connect(cfg.Conn, cfg.Queue, cfg.Threads, threadID)
		if chErr != nil {
			FailOnError(chErr, "couldn't connect to message queue")
		}

		msgs, consumeErr := ch.Consume(cfg.Queue, fmt.Sprintf("%s_%v", cfg.Queue, threadID), false, false, false, false, nil)
		if consumeErr != nil {
			log.Error(fmt.Errorf("failed to register a consumer: %w", consumeErr))
			return
		}

		for j := range msgs {
			// processMessage returns true when the channel should be torn down and
			// re-established from the top of the outer loop.
			if reconnect := processMessage(cfg, j); reconnect {
				break
			}
			if once {
				return
			}
		}
	}
}

// storeBucket persists a scanned bucket when DB writes are enabled, logging any error.
func storeBucket(cfg MQConfig, b *bucket.Bucket) {
	if !cfg.WriteToDB {
		return
	}
	if dbErr := db.StoreBucket(b); dbErr != nil {
		log.Error(dbErr)
	}
}

// enumerate runs object enumeration for a readable bucket, rejecting the message on error.
func enumerate(cfg MQConfig, j amqp.Delivery, b *bucket.Bucket) {
	log.WithFields(log.Fields{"method": "main.mqwork()",
		"bucket_name": b.Name, "region": b.Region}).Debugf("enumerating objects...")

	enumErr := cfg.Provider.Enumerate(b)
	if enumErr != nil {
		log.Errorf("Error enumerating bucket '%s': %v\nEnumerated objects: %v", b.Name, enumErr, len(b.Objects))
		FailOnError(j.Reject(false), "failed to reject")
	}
}

// processMessage handles a single MQ delivery. It returns true when the caller
// should break out of the consume loop and re-establish the channel.
func processMessage(cfg MQConfig, j amqp.Delivery) bool {
	bucketToScan := bucket.Bucket{}

	if unmarshalErr := json.Unmarshal(j.Body, &bucketToScan); unmarshalErr != nil {
		log.Error(unmarshalErr)
	}

	if !bucket.IsValidS3BucketName(bucketToScan.Name) {
		log.Info(fmt.Sprintf("invalid   | %s", bucketToScan.Name))
		FailOnError(j.Ack(false), "failed to ack")
		return false
	}

	b, existsErr := cfg.Provider.BucketExists(&bucketToScan)
	if existsErr != nil {
		log.WithFields(log.Fields{"bucket": b.Name, "step": "checkExists"}).Error(existsErr)
		FailOnError(j.Reject(false), "failed to reject")
	}
	if b.Exists == bucket.BucketNotExist {
		// ack the message and skip to the next
		log.Infof("not_exist | %s", b.Name)
		FailOnError(j.Ack(false), "failed to ack")
		return false
	}

	scanErr := cfg.Provider.Scan(b, false)
	if scanErr != nil {
		log.WithFields(log.Fields{"bucket": b}).Error(scanErr)
		FailOnError(j.Reject(false), "failed to reject")
		return false
	}

	if cfg.DoEnumerate {
		if b.PermAllUsersRead != bucket.PermissionAllowed {
			PrintResult(&bucketToScan, false)
			FailOnError(j.Ack(false), "failed to ack")
			storeBucket(cfg, &bucketToScan)
			return false
		}
		enumerate(cfg, j, b)
	}

	PrintResult(&bucketToScan, false)
	ackErr := j.Ack(false)
	if ackErr != nil {
		// Acknowledge mq message. May fail if we've taken too long and the server has closed the channel
		// If it has, we break and start at the top of the outer for-loop again which re-establishes a new
		// channel
		log.WithFields(log.Fields{"bucket": b}).Error(ackErr)
		return true
	}

	// Write to database
	storeBucket(cfg, &bucketToScan)
	return false
}
