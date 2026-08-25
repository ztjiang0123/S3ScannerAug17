package worker

import (
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/sa7mon/s3scanner/bucket"
	"github.com/sa7mon/s3scanner/db"
	"github.com/sa7mon/s3scanner/provider"
	log "github.com/sirupsen/logrus"
	"sync"
)

func PrintResult(b *bucket.Bucket, json bool) {
	if json {
		log.WithField("bucket", b).Info()
		return
	}

	if b.Exists == bucket.BucketNotExist {
		log.Infof("not_exist | %s", b.Name)
		return
	}

	result := fmt.Sprintf("exists    | %v | %v | %v", b.Name, b.Region, b.String())
	if b.ObjectsEnumerated {
		result = fmt.Sprintf("%v | %v objects (%v)", result, len(b.Objects), humanize.Bytes(b.BucketSize))
	}
	log.Info(result)
}

// Options controls how Work processes each bucket.
type Options struct {
	DoEnumerate bool
	WriteToDB   bool
	JSON        bool
}

func Work(wg *sync.WaitGroup, buckets chan bucket.Bucket, provider provider.StorageProvider, opts Options) {
	defer wg.Done()
	for b1 := range buckets {
		b, existsErr := provider.BucketExists(&b1)
		if existsErr != nil {
			log.Errorf("error     | %s | %s", b.Name, existsErr.Error())
			continue
		}

		if b.Exists == bucket.BucketNotExist {
			PrintResult(b, opts.JSON)
			continue
		}

		// Scan permissions
		scanErr := provider.Scan(b, false)
		if scanErr != nil {
			log.WithFields(log.Fields{"bucket": b}).Error(scanErr)
		}

		if opts.DoEnumerate && b.PermAllUsersRead == bucket.PermissionAllowed {
			log.WithFields(log.Fields{"method": "main.work()",
				"bucket_name": b.Name, "region": b.Region}).Debugf("enumerating objects...")
			enumErr := provider.Enumerate(b)
			if enumErr != nil {
				log.Errorf("Error enumerating bucket '%s': %v\nEnumerated objects: %v", b.Name, enumErr, len(b.Objects))
				continue
			}
		}
		PrintResult(b, opts.JSON)

		if opts.WriteToDB {
			dbErr := db.StoreBucket(b)
			if dbErr != nil {
				log.Error(dbErr)
			}
		}
	}
}
