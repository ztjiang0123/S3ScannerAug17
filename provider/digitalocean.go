package provider

import (
	"github.com/sa7mon/s3scanner/bucket"
)

type DigitalOcean struct {
	regionClients
}

func (pdo DigitalOcean) Insecure() bool {
	return false
}

func (pdo DigitalOcean) Name() string {
	return "digitalocean"
}

func (pdo DigitalOcean) AddressStyle() int {
	return PathStyle
}

func (pdo DigitalOcean) BucketExists(b *bucket.Bucket) (*bucket.Bucket, error) {
	b.Provider = pdo.Name()
	exists, region, err := bucketExists(pdo.clients, b)
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

func (pdo DigitalOcean) Scan(bucket *bucket.Bucket, doDestructiveChecks bool) error {
	client := pdo.getRegionClient(bucket.Region)
	return checkPermissions(client, bucket, doDestructiveChecks)
}

func (pdo DigitalOcean) Enumerate(b *bucket.Bucket) error {
	return enumerateBucketObjects(pdo.getRegionClient(b.Region), b)
}

func NewDigitalOcean() (*DigitalOcean, error) {
	return newRegionalProvider(new(DigitalOcean), "https://%s.digitaloceanspaces.com")
}
