package gcs

import (
	"context"
	"io"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

type hmacClient struct {
	client *minio.Client
	opts   *Config
	log    log.LogEvent
}

func newHMACClient(opts *Config, l log.LogEvent) (*hmacClient, error) {
	if opts.Credentials.HMACAccessKey == "" || opts.Credentials.HMACSecret == "" {
		return nil, errors.New("HMACAccessKey and HMACSecret are required for HMAC GCS credentials")
	}

	if opts.Retryer != nil {
		if opts.Retryer.BackoffInitial > 0 {
			minio.DefaultRetryUnit = opts.Retryer.BackoffInitial
		}
		if opts.Retryer.BackoffMax > 0 {
			minio.DefaultRetryCap = opts.Retryer.BackoffMax
		}
	}

	minioClient, err := minio.New("storage.googleapis.com", &minio.Options{
		Creds: credentials.NewStaticV2(opts.Credentials.HMACAccessKey, opts.Credentials.HMACSecret, ""),
	})
	if err != nil {
		return nil, errors.Wrap(err, "create minio client for GCS HMAC")
	}

	return &hmacClient{
		client: minioClient,
		opts:   opts,
		log:    l,
	}, nil
}

func (h hmacClient) save(name string, data io.Reader, options ...storage.Option) error {
	opts := storage.GetDefaultOpts()
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return errors.Wrap(err, "processing options for save")
		}
	}

	partSize := storage.ComputePartSize(
		opts.Size,
		10<<20, // default: 10 MiB
		5<<20,  // min GCS XML part size
		10_000,
		int64(h.opts.ChunkSize),
	)

	if h.log != nil && opts.UseLogger {
		h.log.Debug(`uploading %q [size hint: %v (%v); part size: %v (%v)]`,
			name,
			opts.Size, storage.PrettySize(opts.Size),
			partSize, storage.PrettySize(partSize))
	}

	putOpts := minio.PutObjectOptions{
		PartSize:   uint64(partSize),
		NumThreads: uint(max(runtime.NumCPU()/2, 1)),
	}

	_, err := h.client.PutObject(
		context.Background(),
		h.opts.Bucket,
		path.Join(h.opts.Prefix, name),
		data,
		-1,
		putOpts,
	)
	if err != nil {
		return errors.Wrap(err, "PutObject")
	}
	return nil
}

func (h hmacClient) fileStat(name string) (storage.FileInfo, error) {
	objectName := path.Join(h.opts.Prefix, name)

	object, err := h.client.StatObject(context.Background(), h.opts.Bucket, objectName, minio.StatObjectOptions{})
	if err != nil {
		respErr := minio.ToErrorResponse(err)
		if respErr.Code == "NoSuchKey" || respErr.Code == "NotFound" {
			return storage.FileInfo{}, storage.ErrNotExist
		}

		return storage.FileInfo{}, errors.Wrap(err, "get object")
	}

	inf := storage.FileInfo{
		Name: name,
		Size: object.Size,
	}

	if inf.Size == 0 {
		return inf, storage.ErrEmpty
	}

	return inf, nil
}

func (h hmacClient) list(prefix, suffix string) ([]storage.FileInfo, error) {
	ctx := context.Background()

	var files []storage.FileInfo

	for obj := range h.client.ListObjects(ctx, h.opts.Bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, errors.Wrap(obj.Err, "list objects")
		}

		name := strings.TrimPrefix(obj.Key, prefix)
		if len(name) > 0 && name[0] == '/' {
			name = name[1:]
		}

		if suffix != "" && !strings.HasSuffix(name, suffix) {
			continue
		}

		files = append(files, storage.FileInfo{
			Name: name,
			Size: obj.Size,
		})
	}

	return files, nil
}

func (h hmacClient) delete(name string) error {
	ctx := context.Background()
	objectName := path.Join(h.opts.Prefix, name)

	err := h.client.RemoveObject(ctx, h.opts.Bucket, objectName, minio.RemoveObjectOptions{})
	if err != nil {
		respErr := minio.ToErrorResponse(err)
		if respErr.Code == "NoSuchKey" || respErr.Code == "NotFound" {
			return storage.ErrNotExist
		}

		return err
	}

	return nil
}

func (h hmacClient) copy(src, dst string) error {
	ctx := context.Background()

	_, err := h.client.CopyObject(ctx,
		minio.CopyDestOptions{
			Bucket: h.opts.Bucket,
			Object: path.Join(h.opts.Prefix, dst),
		},
		minio.CopySrcOptions{
			Bucket: h.opts.Bucket,
			Object: path.Join(h.opts.Prefix, src),
		},
	)

	return err
}

func (h hmacClient) getPartialObject(name string, buf *storage.Arena, start, length int64) (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	objectName := path.Join(h.opts.Prefix, name)

	opts := minio.GetObjectOptions{}

	err := opts.SetRange(start, start+length-1)
	if err != nil {
		return nil, errors.Wrap(err, "failed to set range on GetObjectOptions")
	}

	object, err := h.client.GetObject(ctx, h.opts.Bucket, objectName, opts)
	if err != nil {
		respErr := minio.ToErrorResponse(err)
		if respErr.Code == "NoSuchKey" || respErr.Code == "InvalidRange" {
			return nil, io.EOF
		}

		return nil, storage.GetObjError{Err: err}
	}

	return object, nil
}
