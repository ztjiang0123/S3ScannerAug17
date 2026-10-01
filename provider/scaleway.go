package provider

import (
	"github.com/sa7mon/s3scanner/bucket"
)

type Scaleway struct {
	regionClients
}

func NewProviderScaleway() (*Scaleway, error) {
	return newRegionalProvider(new(Scaleway), "https://s3.%s.scw.cloud")
}

func (sc *Scaleway) Scan(b *bucket.Bucket, doDestructiveChecks bool) error {
	client := sc.getRegionClient(b.Region)
	return checkPermissions(client, b, doDestructiveChecks)
}

func (*Scaleway) Insecure() bool {
	return false
}

func (*Scaleway) Name() string {
	return "scaleway"
}

func (*Scaleway) AddressStyle() int {
	return PathStyle
}

func (sc *Scaleway) BucketExists(b *bucket.Bucket) (*bucket.Bucket, error) {
	b.Provider = sc.Name()
	exists, region, err := bucketExists(sc.clients, b)
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

func (sc *Scaleway) Enumerate(b *bucket.Bucket) error {
	return enumerateBucketObjects(sc.getRegionClient(b.Region), b)
}
