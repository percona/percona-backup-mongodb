package oss

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strings"

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
	opts := storage.GetDefaultOpts()
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return errors.Wrap(err, "processing options for save")
		}
	}

	partSize := storage.ComputePartSize(
		opts.Size,
		defaultPartSize,
		minPartSize,
		int64(o.cfg.MaxUploadParts),
		int64(o.cfg.UploadPartSize),
	)

	if o.log != nil && opts.UseLogger {
		o.log.Debug("uploading %q [size hint: %v (%v); part size: %v (%v)]",
			name,
			opts.Size,
			storage.PrettySize(opts.Size),
			partSize,
			storage.PrettySize(partSize))
	}

	key := path.Join(o.cfg.Prefix, name)

	// Use multipart upload
	initResult, err := o.ossCli.InitiateMultipartUpload(context.Background(), &oss.InitiateMultipartUploadRequest{
		Bucket: oss.Ptr(o.cfg.Bucket),
		Key:    oss.Ptr(key),
	})
	if err != nil {
		return errors.Wrap(err, "initiate multipart upload")
	}
	uploadID := initResult.UploadId

	var completeParts []oss.UploadPart
	partNumber := int32(1)
	buf := make([]byte, partSize)

	for {
		n, err := data.Read(buf)
		if n > 0 {
			uploadPartResult, uerr := o.ossCli.UploadPart(context.Background(), &oss.UploadPartRequest{
				Bucket:     oss.Ptr(o.cfg.Bucket),
				Key:        oss.Ptr(key),
				UploadId:   uploadID,
				PartNumber: partNumber,
				Body:       bytes.NewReader(buf[:n]),
			})
			if uerr != nil {
				_, _ = o.ossCli.AbortMultipartUpload(context.Background(), &oss.AbortMultipartUploadRequest{
					Bucket:   oss.Ptr(o.cfg.Bucket),
					Key:      oss.Ptr(key),
					UploadId: uploadID,
				})
				return errors.Wrap(uerr, "upload part")
			}
			completeParts = append(completeParts, oss.UploadPart{
				ETag:       uploadPartResult.ETag,
				PartNumber: partNumber,
			})
			partNumber++
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			_, _ = o.ossCli.AbortMultipartUpload(context.Background(), &oss.AbortMultipartUploadRequest{
				Bucket:   oss.Ptr(o.cfg.Bucket),
				Key:      oss.Ptr(key),
				UploadId: uploadID,
			})
			return errors.Wrap(err, "read chunk")
		}
	}

	_, err = o.ossCli.CompleteMultipartUpload(context.Background(), &oss.CompleteMultipartUploadRequest{
		Bucket:   oss.Ptr(o.cfg.Bucket),
		Key:      oss.Ptr(key),
		UploadId: uploadID,
		CompleteMultipartUpload: &oss.CompleteMultipartUpload{
			Parts: completeParts,
		},
	})

	return errors.Wrap(err, "complete multipart upload")
}

func (o *OSS) SourceReader(name string) (io.ReadCloser, error) {
	res, err := o.ossCli.GetObject(context.Background(), &oss.GetObjectRequest{
		Bucket: oss.Ptr(o.cfg.Bucket),
		Key:    oss.Ptr(path.Join(o.cfg.Prefix, name)),
	})
	if err != nil {
		var serr *oss.ServiceError
		if errors.As(err, &serr) && serr.Code == "NoSuchKey" {
			return nil, storage.ErrNotExist
		}
		return nil, errors.Wrap(err, "get object")
	}

	return res.Body, nil
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
	prfx := path.Join(o.cfg.Prefix, prefix)
	if prfx != "" && !strings.HasSuffix(prfx, "/") {
		prfx += "/"
	}

	var files []storage.FileInfo
	var continuationToken *string
	for {
		res, err := o.ossCli.ListObjectsV2(context.Background(), &oss.ListObjectsV2Request{
			Bucket:            oss.Ptr(o.cfg.Bucket),
			Prefix:            oss.Ptr(prfx),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, errors.Wrap(err, "list OSS objects")
		}
		for _, obj := range res.Contents {
			key := ""
			if obj.Key != nil {
				key = *obj.Key
			}

			f := strings.TrimPrefix(key, prfx)
			if len(f) == 0 {
				continue
			}
			if f[0] == '/' {
				f = f[1:]
			}

			if strings.HasSuffix(f, suffix) {
				files = append(files, storage.FileInfo{
					Name: f,
					Size: obj.Size,
				})
			}
		}
		if res.IsTruncated {
			continuationToken = res.NextContinuationToken
			continue
		}
		break
	}
	return files, nil
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
	_, err := o.ossCli.CopyObject(context.Background(), &oss.CopyObjectRequest{
		Bucket:       oss.Ptr(o.cfg.Bucket),
		Key:          oss.Ptr(path.Join(o.cfg.Prefix, dst)),
		SourceBucket: oss.Ptr(o.cfg.Bucket),
		SourceKey:    oss.Ptr(path.Join(o.cfg.Prefix, src)),
	})

	return errors.Wrap(err, "copy object")
}
