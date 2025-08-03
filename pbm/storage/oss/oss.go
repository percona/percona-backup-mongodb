package oss

import (
	"context"
	"fmt"
	"io"
	"path"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

var _ storage.Storage = &OSS{}

func New(cfg *Config, node string, l log.LogEvent) (*OSS, error) {
	if err := cfg.Cast(); err != nil {
		return nil, fmt.Errorf("cast config: %w", err)
	}

	client, err := configureClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("configure client: %w", err)
	}

	o := &OSS{
		cfg:    cfg,
		node:   node,
		log:    l,
		ossCli: client,
	}

	return o, nil
}

type OSS struct {
	cfg    *Config
	node   string
	log    log.LogEvent
	ossCli *oss.Client
}

func (o *OSS) Type() storage.Type {
	return storage.OSS
}

func (o *OSS) Save(name string, data io.Reader, options ...storage.Option) error {
	return nil
}

func (o *OSS) SourceReader(name string) (io.ReadCloser, error) {
	return nil, nil
}

// FileStat returns file info. It returns error if file is empty or not exists.
func (o *OSS) FileStat(name string) (storage.FileInfo, error) {
	inf := storage.FileInfo{}

	res, err := o.ossCli.HeadObject(context.Background(), &oss.HeadObjectRequest{
		Bucket: oss.Ptr(o.cfg.Bucket),
		Key:    oss.Ptr(path.Join(o.cfg.Prefix, name)),
	})
	if err != nil {
		var serr *oss.ServiceError
		if errors.As(err, &serr) && serr.Code == "NoSuchKey" {
			return inf, storage.ErrNotExist
		}
		return inf, errors.Wrap(err, "get OSS object header")
	}

	inf.Name = name
	inf.Size = res.ContentLength
	if inf.Size == 0 {
		return inf, storage.ErrEmpty
	}

	return inf, nil
}

// List scans path with prefix and returns all files with given suffix.
// Both prefix and suffix can be omitted.
func (o *OSS) List(prefix, suffix string) ([]storage.FileInfo, error) {
	return nil, nil
}

// Delete deletes given file.
// It returns storage.ErrNotExist if a file doesn't exists.
func (o *OSS) Delete(name string) error {
	key := path.Join(o.cfg.Prefix, name)
	path.Join(o.cfg.Prefix, name)
	_, err := o.ossCli.DeleteObject(context.Background(), &oss.DeleteObjectRequest{
		Bucket: oss.Ptr(o.cfg.Bucket),
		Key:    oss.Ptr(key),
	})
	if err != nil {
		return errors.Wrapf(err, "delete %s/%s file from OSS", o.cfg.Bucket, key)
	}
	return nil
}

// Copy makes a copy of the src objec/file under dst name
func (o *OSS) Copy(src, dst string) error {
	return nil
}
