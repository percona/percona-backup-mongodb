package gcs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/signer"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

type hmacClient struct {
	client *minio.Client
	opts   *Config
}

func newHmacClient(opts *Config) (*hmacClient, error) {
	if opts.Credentials.HMACAccessKey == "" || opts.Credentials.HMACSecret == "" {
		return nil, errors.New("HMACAccessKey and HMACSecret are required for HMAC GCS credentials")
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
	}, nil
}

func (h hmacClient) save(name string, data io.Reader, _ ...storage.Option) error {
	objectKey := path.Join(h.opts.Prefix, name)

	opts := minio.PutObjectOptions{}
	if h.opts.ChunkSize != nil && *h.opts.ChunkSize > 0 {
		opts.PartSize = uint64(*h.opts.ChunkSize)
	}

	_, err := h.client.PutObject(
		context.Background(), h.opts.Bucket, objectKey, data, -1, opts,
	)

	if err != nil {
		return errors.Wrap(err, "PutObject")
	}
	return nil
}

func (h hmacClient) save_bu(name string, data io.Reader, _ ...storage.Option) error {
	objectKey := path.Join(h.opts.Prefix, name)

	encodePath := func(bucket, key string) string {
		segs := strings.Split(key, "/")
		for i, s := range segs {
			segs[i] = url.PathEscape(s)
		}
		return fmt.Sprintf("https://storage.googleapis.com/%s/%s",
			bucket, strings.Join(segs, "/"))
	}
	rawURL := encodePath(h.opts.Bucket, objectKey)

	req, err := http.NewRequest(http.MethodPut, rawURL, nil)
	if err != nil {
		return errors.Wrap(err, "build request")
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	const ttl = 24 * time.Hour
	signed := signer.PreSignV2(
		*req,
		h.opts.Credentials.HMACAccessKey,
		h.opts.Credentials.HMACSecret,
		int64(ttl.Seconds()),
		false,
	)

	signed.Body = io.NopCloser(data)
	signed.ContentLength = -1

	resp, err := http.DefaultClient.Do(signed)
	if err != nil {
		return errors.Wrap(err, "PUT (chunked)")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload failed: %s – %s", resp.Status, strings.TrimSpace(string(body)))
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

func (h hmacClient) getPartialObject(ctx context.Context, name string, start, length int64) (io.ReadCloser, error) {
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
