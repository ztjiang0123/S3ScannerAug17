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

// messageOutcome tells the consumer loop how to proceed after a single
// message has been processed.
type messageOutcome int

const (
	// outcomeNext moves on to the next message on the current channel after a
	// message was skipped early (invalid, missing, or scan error).
	outcomeNext messageOutcome = iota
	// outcomeDone signals a message was fully scanned and acknowledged.
	outcomeDone
	// outcomeReconnect abandons the current channel and re-establishes it.
	outcomeReconnect
	// outcomeStop ends the worker entirely (used in test mode).
	outcomeStop
)

func WorkMQ(threadID int, wg *sync.WaitGroup, cfg MQConfig) {
	queue := cfg.Queue
	_, once := os.LookupEnv("TEST_MQ") // If we're being tested, exit after one bucket is scanned
	defer wg.Done()

	// Wrap the whole thing in a for (while) loop so if the mq server kills the channel, we start it up again
	for {
		if consume(threadID, cfg, queue, once) == outcomeStop {
			return
		}
	}
}

// consume establishes a channel, consumes messages until the channel is lost
// or the worker is told to stop, and reports how the outer loop should proceed.
func consume(threadID int, cfg MQConfig, queue string, once bool) messageOutcome {
	ch, chErr := mq.Connect(cfg.Conn, queue, cfg.Threads, threadID)
	if chErr != nil {
		FailOnError(chErr, "couldn't connect to message queue")
	}

	msgs, consumeErr := ch.Consume(queue, fmt.Sprintf("%s_%v", queue, threadID), false, false, false, false, nil)
	if consumeErr != nil {
		log.Error(fmt.Errorf("failed to register a consumer: %w", consumeErr))
		return outcomeStop
	}

	for j := range msgs {
		outcome := processMessage(cfg, j)
		if outcome == outcomeReconnect {
			return outcomeReconnect
		}
		// In test mode (`once`) we exit as soon as a bucket has been fully scanned.
		if once && outcome == outcomeDone {
			return outcomeStop
		}
	}
	return outcomeReconnect
}

// processMessage scans a single bucket message and acknowledges or rejects it.
func processMessage(cfg MQConfig, j amqp.Delivery) messageOutcome {
	bucketToScan := bucket.Bucket{}
	if unmarshalErr := json.Unmarshal(j.Body, &bucketToScan); unmarshalErr != nil {
		log.Error(unmarshalErr)
	}

	if !bucket.IsValidS3BucketName(bucketToScan.Name) {
		log.Info(fmt.Sprintf("invalid   | %s", bucketToScan.Name))
		FailOnError(j.Ack(false), "failed to ack")
		return outcomeNext
	}

	b, scanned := scanBucket(cfg.Provider, &bucketToScan, j)
	if !scanned {
		return outcomeNext
	}

	if cfg.DoEnumerate && !enumerateBucket(cfg, &bucketToScan, b, j) {
		return outcomeNext
	}

	PrintResult(&bucketToScan, false)
	if ackErr := j.Ack(false); ackErr != nil {
		// Acknowledge mq message. May fail if we've taken too long and the server has closed the channel.
		// If it has, we reconnect at the top of the outer for-loop which re-establishes a new channel.
		log.WithFields(log.Fields{"bucket": b}).Error(ackErr)
		return outcomeReconnect
	}

	storeBucket(cfg, &bucketToScan)
	return outcomeDone
}

// scanBucket confirms the bucket exists and scans it. It returns the resolved
// bucket and whether processing should continue for this message.
func scanBucket(p provider.StorageProvider, bucketToScan *bucket.Bucket, j amqp.Delivery) (*bucket.Bucket, bool) {
	b, existsErr := p.BucketExists(bucketToScan)
	if existsErr != nil {
		log.WithFields(log.Fields{"bucket": b.Name, "step": "checkExists"}).Error(existsErr)
		FailOnError(j.Reject(false), "failed to reject")
	}
	if b.Exists == bucket.BucketNotExist {
		// ack the message and skip to the next
		log.Infof("not_exist | %s", b.Name)
		FailOnError(j.Ack(false), "failed to ack")
		return b, false
	}

	if scanErr := p.Scan(b, false); scanErr != nil {
		log.WithFields(log.Fields{"bucket": b}).Error(scanErr)
		FailOnError(j.Reject(false), "failed to reject")
		return b, false
	}
	return b, true
}

// enumerateBucket enumerates a readable bucket's objects. It returns false when
// the message has already been handled and the caller should move on.
func enumerateBucket(cfg MQConfig, bucketToScan, b *bucket.Bucket, j amqp.Delivery) bool {
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

// storeBucket persists the bucket when database writes are enabled.
func storeBucket(cfg MQConfig, bucketToScan *bucket.Bucket) {
	if !cfg.WriteToDB {
		return
	}
	if dbErr := db.StoreBucket(bucketToScan); dbErr != nil {
		log.Error(dbErr)
	}
}
