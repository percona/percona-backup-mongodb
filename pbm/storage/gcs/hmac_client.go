package gcs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

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
		Creds: credentials.NewStaticV4(opts.Credentials.HMACAccessKey, opts.Credentials.HMACSecret, ""),
	})
	if err != nil {
		return nil, errors.Wrap(err, "create minio client for GCS HMAC")
	}

	return &hmacClient{
		client: minioClient,
		opts:   opts,
	}, nil
}

func (h hmacClient) save_new(name string, data io.Reader, options ...storage.Option) error {
	fmt.Println("--- SAVE", name)

	opts := storage.GetDefaultOpts()
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return errors.Wrap(err, "processing options for save")
		}
	}

	objectName := path.Join(h.opts.Prefix, name)
	partSize := 10 * 1024 * 1024 // 10MB

	putOpts := minio.PutObjectOptions{
		PartSize: uint64(partSize),
	}

	var (
		size   int64
		reader io.Reader
	)

	if opts.Size >= 0 {
		fmt.Println("does this part!, know the size")
		size = opts.Size
		reader = data
	} else if rs, ok := data.(io.ReadSeeker); ok {
		cur, _ := rs.Seek(0, io.SeekCurrent)
		end, _ := rs.Seek(0, io.SeekEnd)
		size = end - cur
		_, _ = rs.Seek(cur, io.SeekStart)

		reader = rs
	} else {
		return errors.New("unable to determine object size: provide Size() option or use io.ReadSeeker")
	}

	//fmt.Println("--- STATS: ", opts.Size, size, reader, data)

	_, err := h.client.PutObject(
		context.Background(),
		h.opts.Bucket,
		objectName,
		reader,
		size,
		putOpts,
	)

	if err != nil {
		fmt.Println("Upload error:", err)
		return errors.Wrap(err, "upload via HMAC")
	}

	fmt.Println("Upload successful:", objectName)
	return nil
}

//func (h hmacClient) save(name string, data io.Reader, _ ...storage.Option) error {
//	const defaultPart = 64 << 20 // 64 MiB
//	part := defaultPart
//	if h.opts.ChunkSize != nil && *h.opts.ChunkSize > 0 {
//		part = *h.opts.ChunkSize
//	}
//
//	_, err := h.client.PutObject(
//		context.Background(),
//		h.opts.Bucket,
//		path.Join(h.opts.Prefix, name),
//		data, // UNTOUCHED reader
//		-1,   // unknown size  → streaming multipart
//		minio.PutObjectOptions{PartSize: uint64(part)},
//	)
//	return errors.Wrap(err, "upload via HMAC")
//}

func (h hmacClient) save(name string, data io.Reader, _ ...storage.Option) error {
	fmt.Println("--- SAVE", name)

	var (
		rdr         = data
		seeker, sOK = data.(io.Seeker)
		size        int64
	)

	if sOK {
		cur, _ := seeker.Seek(0, io.SeekCurrent)
		end, _ := seeker.Seek(0, io.SeekEnd)
		size = end - cur
		_, _ = seeker.Seek(cur, io.SeekStart)
	} else {
		// Try to read small object into memory
		const small = 16 << 20
		buf, err := io.ReadAll(io.LimitReader(data, small+1))
		if err != nil {
			return errors.Wrap(err, "read small object buffer")
		}
		if int64(len(buf)) <= small {
			size = int64(len(buf))
			rdr = bytes.NewReader(buf)
		} else {
			// Fallback to temp file
			tmp, err := os.CreateTemp("", "pbm-upload-*")
			if err != nil {
				return errors.Wrap(err, "create temp file")
			}
			defer func() {
				tmp.Close()
				os.Remove(tmp.Name())
			}()
			n, err := io.Copy(tmp, io.MultiReader(bytes.NewReader(buf), data))
			if err != nil {
				return errors.Wrap(err, "buffer stream to temp file")
			}
			_, err = tmp.Seek(0, io.SeekStart)
			if err != nil {
				return errors.Wrap(err, "seek temp file")
			}
			rdr = tmp
			size = n
		}
	}

	// ----------------------------------------------------------- Part sizing
	partSize := int64(64 << 20) // 64 MiB default
	if h.opts.ChunkSize != nil && *h.opts.ChunkSize > 0 {
		partSize = int64(*h.opts.ChunkSize)
	}

	putOpts := minio.PutObjectOptions{
		PartSize:         uint64(partSize),
		DisableMultipart: size >= 0 && size <= partSize,
	}

	objectName := path.Join(h.opts.Prefix, name)

	//options := minio.PutObjectOptions{}
	//if h.opts.ChunkSize != nil && *h.opts.ChunkSize > 0 {
	//	options.PartSize = uint64(*h.opts.ChunkSize)
	//}

	//fmt.Println("--- STATS: ", size, rdr, data)

	_, err := h.client.PutObject(
		context.Background(),
		h.opts.Bucket,
		objectName,
		rdr,
		size,
		putOpts,
	)
	return err
}

func (h hmacClient) fileStat(name string) (storage.FileInfo, error) {
	//fmt.Println(h.list("", defs.MetadataFileSuffix))

	objectName := path.Join(h.opts.Prefix, name)

	object, err := h.client.StatObject(context.Background(), h.opts.Bucket, objectName, minio.StatObjectOptions{})

	//fmt.Println(objectName, object, err)

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

	fmt.Println("--- PREFIX: ", prefix)

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

		fmt.Println(name)

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
	fmt.Println("--- DELETE", name)

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
			Object: path.Join(h.opts.Prefix, src),
		},
		minio.CopySrcOptions{
			Bucket: h.opts.Bucket,
			Object: path.Join(h.opts.Prefix, dst),
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
