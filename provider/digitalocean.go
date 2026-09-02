package provider

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sa7mon/s3scanner/bucket"
	"github.com/sa7mon/s3scanner/provider/clientmap"
)

type DigitalOcean struct {
	clients *clientmap.ClientMap
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
	return checkBucketExists(b, func() (bool, string, error) {
		return bucketExists(pdo.clients, b)
	})
}

func (pdo DigitalOcean) Scan(bucket *bucket.Bucket, doDestructiveChecks bool) error {
	client := pdo.getRegionClient(bucket.Region)
	return checkPermissions(client, bucket, doDestructiveChecks)
}

func (pdo DigitalOcean) Enumerate(b *bucket.Bucket) error {
	return enumerateBucketObjects(pdo.getRegionClient(b.Region), b)
}

func (pdo *DigitalOcean) newClients() (*clientmap.ClientMap, error) {
	return newClientMap(pdo, func(r string) string {
		return fmt.Sprintf("https://%s.digitaloceanspaces.com", r)
	})
}

func (pdo *DigitalOcean) getRegionClient(region string) *s3.Client {
	return pdo.clients.Get(region, false)
}

func NewDigitalOcean() (*DigitalOcean, error) {
	pdo := new(DigitalOcean)

	clients, err := pdo.newClients()
	if err != nil {
		return pdo, err
	}
	pdo.clients = clients
	return pdo, nil
}
