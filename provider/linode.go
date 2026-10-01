package provider

import (
	"github.com/sa7mon/s3scanner/bucket"
)

type Linode struct {
	regionClients
}

func NewProviderLinode() (*Linode, error) {
	return newRegionalProvider(new(Linode), "https://%s.linodeobjects.com")
}

func (pl *Linode) BucketExists(b *bucket.Bucket) (*bucket.Bucket, error) {
	b.Provider = pl.Name()
	exists, region, err := bucketExists(pl.clients, b)
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

func (pl *Linode) Enumerate(b *bucket.Bucket) error {
	return enumerateBucketObjects(pl.getRegionClient(b.Region), b)
}

func (pl *Linode) Scan(b *bucket.Bucket, doDestructiveChecks bool) error {
	client := pl.getRegionClient(b.Region)
	return checkPermissions(client, b, doDestructiveChecks)
}

func (*Linode) Insecure() bool {
	return false
}

func (*Linode) Name() string {
	return "linode"
}

func (*Linode) AddressStyle() int {
	return VirtualHostStyle
}
