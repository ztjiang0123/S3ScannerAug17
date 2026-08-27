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

// msgOutcome tells WorkMQ's outer loop what to do after handling a single message.
type msgOutcome int

const (
	// continueQueue moves on to the next message on the current channel.
	continueQueue msgOutcome = iota
	// reconnect breaks out to re-establish the channel (e.g. the server closed it).
	reconnect
	// stopWorker exits the worker entirely (used by the one-shot test mode).
	stopWorker
)

// storeBucket persists the scanned bucket when database writes are enabled.
func storeBucket(b *bucket.Bucket, writeToDB bool) {
	if !writeToDB {
		return
	}
	if dbErr := db.StoreBucket(b); dbErr != nil {
		log.Error(dbErr)
	}
}

// enumerate handles the object-enumeration step for a bucket that exists and was scanned. It returns
// true when the message has already been fully handled (acked/stored) and the caller should move on.
func enumerate(p provider.StorageProvider, b *bucket.Bucket, j amqp.Delivery, cfg MQConfig) (handled bool) {
	if b.PermAllUsersRead != bucket.PermissionAllowed {
		PrintResult(b, false)
		FailOnError(j.Ack(false), "failed to ack")
		storeBucket(b, cfg.WriteToDB)
		return true
	}

	log.WithFields(log.Fields{"method": "main.mqwork()",
		"bucket_name": b.Name, "region": b.Region}).Debugf("enumerating objects...")

	if enumErr := p.Enumerate(b); enumErr != nil {
		log.Errorf("Error enumerating bucket '%s': %v\nEnumerated objects: %v", b.Name, enumErr, len(b.Objects))
		FailOnError(j.Reject(false), "failed to reject")
	}
	return false
}

// handleMessage processes a single queue delivery and reports how the outer loop should proceed.
func handleMessage(p provider.StorageProvider, j amqp.Delivery, cfg MQConfig, once bool) msgOutcome {
	bucketToScan := bucket.Bucket{}
	if unmarshalErr := json.Unmarshal(j.Body, &bucketToScan); unmarshalErr != nil {
		log.Error(unmarshalErr)
	}

	if !bucket.IsValidS3BucketName(bucketToScan.Name) {
		log.Info(fmt.Sprintf("invalid   | %s", bucketToScan.Name))
		FailOnError(j.Ack(false), "failed to ack")
		return continueQueue
	}

	b, existsErr := p.BucketExists(&bucketToScan)
	if existsErr != nil {
		log.WithFields(log.Fields{"bucket": b.Name, "step": "checkExists"}).Error(existsErr)
		FailOnError(j.Reject(false), "failed to reject")
	}
	if b.Exists == bucket.BucketNotExist {
		// ack the message and skip to the next
		log.Infof("not_exist | %s", b.Name)
		FailOnError(j.Ack(false), "failed to ack")
		return continueQueue
	}

	if scanErr := p.Scan(b, false); scanErr != nil {
		log.WithFields(log.Fields{"bucket": b}).Error(scanErr)
		FailOnError(j.Reject(false), "failed to reject")
		return continueQueue
	}

	if cfg.DoEnumerate {
		if handled := enumerate(p, b, j, cfg); handled {
			return continueQueue
		}
	}

	PrintResult(&bucketToScan, false)
	if ackErr := j.Ack(false); ackErr != nil {
		// Acknowledge mq message. May fail if we've taken too long and the server has closed the channel
		// If it has, we break and start at the top of the outer for-loop again which re-establishes a new
		// channel
		log.WithFields(log.Fields{"bucket": b}).Error(ackErr)
		return reconnect
	}

	storeBucket(&bucketToScan, cfg.WriteToDB)
	if once {
		return stopWorker
	}
	return continueQueue
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

	consume:
		for j := range msgs {
			switch handleMessage(cfg.Provider, j, cfg, once) {
			case stopWorker:
				return
			case reconnect:
				break consume
			case continueQueue:
			}
		}
	}
}
