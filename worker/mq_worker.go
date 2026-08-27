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
	queue := cfg.Queue
	_, once := os.LookupEnv("TEST_MQ") // If we're being tested, exit after one bucket is scanned
	defer wg.Done()

	// Wrap the whole thing in a for (while) loop so if the mq server kills the channel, we start it up again
	for {
		ch, chErr := mq.Connect(cfg.Conn, queue, cfg.Threads, threadID)
		if chErr != nil {
			FailOnError(chErr, "couldn't connect to message queue")
		}

		msgs, consumeErr := ch.Consume(queue, fmt.Sprintf("%s_%v", queue, threadID), false, false, false, false, nil)
		if consumeErr != nil {
			log.Error(fmt.Errorf("failed to register a consumer: %w", consumeErr))
			return
		}

		for j := range msgs {
			// A channel-closed error while acking means the server killed the channel; break so the
			// outer for-loop re-establishes a new one. Any other outcome continues to the next message.
			if channelClosed := processMessage(j, cfg); channelClosed {
				break
			}
			if once {
				return
			}
		}
	}
}

// processMessage scans and (optionally) enumerates a single bucket from an mq delivery. It returns
// true when the delivery could not be acked because the server closed the channel, signalling the
// caller to re-establish the channel.
func processMessage(j amqp.Delivery, cfg MQConfig) (channelClosed bool) {
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

	if cfg.DoEnumerate && !enumerateBucket(j, b, &bucketToScan, cfg) {
		return false
	}

	PrintResult(&bucketToScan, false)
	if ackErr := j.Ack(false); ackErr != nil {
		// Acknowledge mq message. May fail if we've taken too long and the server has closed the channel
		// If it has, we break and start at the top of the outer for-loop again which re-establishes a new
		// channel
		log.WithFields(log.Fields{"bucket": b}).Error(ackErr)
		return true
	}

	storeBucket(cfg, &bucketToScan)
	return false
}

// enumerateBucket handles the DoEnumerate branch for a single bucket. It returns false when the
// caller should stop processing this delivery (the bucket was not publicly readable and has already
// been acked), and true when processing should continue to the final ack/store steps.
func enumerateBucket(j amqp.Delivery, b *bucket.Bucket, bucketToScan *bucket.Bucket, cfg MQConfig) (proceed bool) {
	if b.PermAllUsersRead != bucket.PermissionAllowed {
		PrintResult(bucketToScan, false)
		FailOnError(j.Ack(false), "failed to ack")
		storeBucket(cfg, bucketToScan)
		return false
	}

	log.WithFields(log.Fields{"method": "main.mqwork()",
		"bucket_name": b.Name, "region": b.Region}).Debugf("enumerating objects...")

	if enumErr := cfg.Provider.Enumerate(b); enumErr != nil {
		log.Errorf("Error enumerating bucket '%s': %v\nEnumerated objects: %v", b.Name, enumErr, len(b.Objects))
		FailOnError(j.Reject(false), "failed to reject")
	}
	return true
}

// storeBucket persists the bucket when the worker is configured to write to the database.
func storeBucket(cfg MQConfig, b *bucket.Bucket) {
	if !cfg.WriteToDB {
		return
	}
	if dbErr := db.StoreBucket(b); dbErr != nil {
		log.Error(dbErr)
	}
}
