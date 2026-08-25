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
		msgs, ok := consumeMessages(threadID, cfg)
		if !ok {
			return
		}

		for j := range msgs {
			outcome := processMessage(j, cfg)
			if outcome == outcomeChannelClosed {
				// The server closed the channel (e.g. we took too long to ack). Break out of the
				// message loop so the outer for-loop re-establishes a new channel.
				break
			}
			if once {
				return
			}
		}
	}
}

// messageOutcome describes how far processMessage progressed so the caller can decide
// whether to keep consuming, re-establish the channel, or stop.
type messageOutcome int

const (
	// outcomeHandled means the message was fully processed (acked or rejected).
	outcomeHandled messageOutcome = iota
	// outcomeChannelClosed means acking failed because the server closed the channel.
	outcomeChannelClosed
)

// consumeMessages connects to the queue and registers a consumer. The bool result reports
// whether WorkMQ should keep running (false means a fatal consumer error occurred).
func consumeMessages(threadID int, cfg MQConfig) (<-chan amqp.Delivery, bool) {
	ch, chErr := mq.Connect(cfg.Conn, cfg.Queue, cfg.Threads, threadID)
	if chErr != nil {
		FailOnError(chErr, "couldn't connect to message queue")
	}

	consumerTag := fmt.Sprintf("%s_%v", cfg.Queue, threadID)
	msgs, consumeErr := ch.Consume(cfg.Queue, consumerTag, false, false, false, false, nil)
	if consumeErr != nil {
		log.Error(fmt.Errorf("failed to register a consumer: %w", consumeErr))
		return nil, false
	}
	return msgs, true
}

// processMessage runs a single delivery through validation, existence checks, scanning,
// optional enumeration, and finally acks/rejects the message.
func processMessage(j amqp.Delivery, cfg MQConfig) messageOutcome {
	bucketToScan := bucket.Bucket{}
	if unmarshalErr := json.Unmarshal(j.Body, &bucketToScan); unmarshalErr != nil {
		log.Error(unmarshalErr)
	}

	if !bucket.IsValidS3BucketName(bucketToScan.Name) {
		log.Info(fmt.Sprintf("invalid   | %s", bucketToScan.Name))
		FailOnError(j.Ack(false), "failed to ack")
		return outcomeHandled
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
		return outcomeHandled
	}

	scanErr := cfg.Provider.Scan(b, false)
	if scanErr != nil {
		log.WithFields(log.Fields{"bucket": b}).Error(scanErr)
		FailOnError(j.Reject(false), "failed to reject")
		return outcomeHandled
	}

	if cfg.DoEnumerate && !enumerateBucket(j, b, cfg) {
		return outcomeHandled
	}

	return finishMessage(j, b, &bucketToScan, cfg)
}

// enumerateBucket handles the DoEnumerate branch. It returns false when the message has
// already been handled (bucket not publicly readable) and processing should stop.
func enumerateBucket(j amqp.Delivery, b *bucket.Bucket, cfg MQConfig) bool {
	if b.PermAllUsersRead != bucket.PermissionAllowed {
		PrintResult(b, false)
		FailOnError(j.Ack(false), "failed to ack")
		storeBucket(b, cfg)
		return false
	}

	log.WithFields(log.Fields{"method": "main.mqwork()",
		"bucket_name": b.Name, "region": b.Region}).Debugf("enumerating objects...")

	enumErr := cfg.Provider.Enumerate(b)
	if enumErr != nil {
		log.Errorf("Error enumerating bucket '%s': %v\nEnumerated objects: %v", b.Name, enumErr, len(b.Objects))
		FailOnError(j.Reject(false), "failed to reject")
	}
	return true
}

// finishMessage prints the result, acks the message, and persists it if configured.
func finishMessage(j amqp.Delivery, b, bucketToScan *bucket.Bucket, cfg MQConfig) messageOutcome {
	PrintResult(b, false)

	ackErr := j.Ack(false)
	if ackErr != nil {
		// Acknowledge mq message. May fail if we've taken too long and the server has closed the channel.
		log.WithFields(log.Fields{"bucket": b}).Error(ackErr)
		return outcomeChannelClosed
	}

	storeBucket(bucketToScan, cfg)
	return outcomeHandled
}

// storeBucket persists the bucket to the database when WriteToDB is enabled.
func storeBucket(b *bucket.Bucket, cfg MQConfig) {
	if !cfg.WriteToDB {
		return
	}
	if dbErr := db.StoreBucket(b); dbErr != nil {
		log.Error(dbErr)
	}
}
