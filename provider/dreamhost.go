package provider

import (
	"github.com/sa7mon/s3scanner/bucket"
	"strings"
)

// Dreamhost responds strangely if you attempt to access a bucket named 'auth'
var forbiddenBuckets = []string{"auth"}

type Dreamhost struct {
	regionClients
}

func (p Dreamhost) Insecure() bool {
	return false
}

func (Dreamhost) Name() string {
	return "dreamhost"
}

func (p Dreamhost) AddressStyle() int {
	return PathStyle
}

func (p Dreamhost) BucketExists(b *bucket.Bucket) (*bucket.Bucket, error) {
	// Check for forbidden name
	for _, fb := range forbiddenBuckets {
		if strings.ToLower(b.Name) == fb {
			b.Exists = bucket.BucketNotExist
			return b, nil
		}
	}

	b.Provider = p.Name()
	exists, region, err := bucketExists(p.clients, b)
	if err != nil {
		return b, err
	}
	if exists {
		b.Exists = bucket.BucketExists
		b.Region = region
	} else {
		b.Exists = bucket.BucketNotExist
	}

	return b, nil
}

func (p Dreamhost) Scan(bucket *bucket.Bucket, doDestructiveChecks bool) error {
	client := p.getRegionClient(bucket.Region)
	return checkPermissions(client, bucket, doDestructiveChecks)
}

func (p Dreamhost) Enumerate(b *bucket.Bucket) error {
	return enumerateBucketObjects(p.getRegionClient(b.Region), b)
}

func NewProviderDreamhost() (*Dreamhost, error) {
	return newRegionalProvider(new(Dreamhost), "https://objects-%s.dream.io")
}
