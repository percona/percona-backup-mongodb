package gcs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"

	storagegcs "cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

type googleClient struct {
	bucketHandle *storagegcs.BucketHandle
	opts         *Config
	log          log.LogEvent
}

func newGoogleClient(opts *Config, l log.LogEvent) (*googleClient, error) {
	ctx := context.Background()

	if opts.Credentials.PrivateKey == "" || opts.Credentials.ClientEmail == "" {
		return nil, errors.New("clientEmail and privateKey are required for GCS credentials")
	}

	creds, err := json.Marshal(ServiceAccountCredentials{
		Type:                "service_account",
		PrivateKey:          opts.Credentials.PrivateKey,
		ClientEmail:         opts.Credentials.ClientEmail,
		AuthURI:             "https://accounts.google.com/o/oauth2/auth",
		TokenURI:            "https://oauth2.googleapis.com/token",
		UniverseDomain:      "googleapis.com",
		AuthProviderCertURL: "https://www.googleapis.com/oauth2/v1/certs",
		ClientCertURL: fmt.Sprintf(
			"https://www.googleapis.com/robot/v1/metadata/x509/%s",
			opts.Credentials.ClientEmail,
		),
	})
	if err != nil {
		return nil, errors.Wrap(err, "marshal GCS credentials")
	}

	cli, err := storagegcs.NewClient(ctx, option.WithCredentialsJSON(creds))
	if err != nil {
		return nil, errors.Wrap(err, "new GCS client")
	}

	bh := cli.Bucket(opts.Bucket)

	if opts.Retryer != nil {
		bh = bh.Retryer(
			storagegcs.WithBackoff(gax.Backoff{
				Initial:    opts.Retryer.BackoffInitial,
				Max:        opts.Retryer.BackoffMax,
				Multiplier: opts.Retryer.BackoffMultiplier,
			}),

			storagegcs.WithPolicy(storagegcs.RetryAlways),
		)
	}

	return &googleClient{
		bucketHandle: bh,
		opts:         opts,
		log:          l,
	}, nil
}

func (g googleClient) save(name string, data io.Reader, options ...storage.Option) error {
	opts := storage.GetDefaultOpts()
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return errors.Wrap(err, "processing options for save")
		}
	}

	const align int64 = 256 << 10 // 256 KiB (both min size and alignment)

	partSize := storage.ComputePartSize(
		opts.Size,
		10<<20, // default: 10 MiB
		align,
		10_000,
		int64(g.opts.ChunkSize),
	)

	if rem := partSize % align; rem != 0 {
		partSize += align - rem
	}

	if g.log != nil && opts.UseLogger {
		g.log.Debug(`uploading %q [size hint: %v (%v); part size: %v (%v)]`,
			name,
			opts.Size, storage.PrettySize(opts.Size),
			partSize, storage.PrettySize(partSize))
	}

	ctx := context.Background()
	w := g.bucketHandle.Object(path.Join(g.opts.Prefix, name)).NewWriter(ctx)
	w.ChunkSize = int(partSize)
	if g.log != nil && opts.UseLogger {
		w.ProgressFunc = func(written int64) {
			if opts.Size > 0 {
				g.log.Debug("uploaded %v / %v (%.1f%%)",
					written, opts.Size,
					float64(written)*100/float64(opts.Size))
			} else {
				g.log.Debug("uploaded %v (total unknown)", written)
			}
		}
	}

	if _, err := io.Copy(w, data); err != nil {
		return errors.Wrap(err, "save data")
	}

	if err := w.Close(); err != nil {
		return errors.Wrap(err, "writer close")
	}

	return nil
}

func (g googleClient) fileStat(name string) (storage.FileInfo, error) {
	ctx := context.Background()

	attrs, err := g.bucketHandle.Object(path.Join(g.opts.Prefix, name)).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storagegcs.ErrObjectNotExist) {
			return storage.FileInfo{}, storage.ErrNotExist
		}

		return storage.FileInfo{}, errors.Wrap(err, "get properties")
	}

	inf := storage.FileInfo{
		Name: attrs.Name,
		Size: attrs.Size,
	}

	if inf.Size == 0 {
		return inf, storage.ErrEmpty
	}

	return inf, nil
}

func (g googleClient) list(prefix, suffix string) ([]storage.FileInfo, error) {
	ctx := context.Background()

	var files []storage.FileInfo
	it := g.bucketHandle.Objects(ctx, &storagegcs.Query{Prefix: prefix})

	for {
		attrs, err := it.Next()

		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return nil, errors.Wrap(err, "list objects")
		}

		name := attrs.Name
		name = strings.TrimPrefix(name, prefix)
		if len(name) == 0 {
			continue
		}
		if name[0] == '/' {
			name = name[1:]
		}

		if suffix != "" && !strings.HasSuffix(name, suffix) {
			continue
		}

		files = append(files, storage.FileInfo{
			Name: name,
			Size: attrs.Size,
		})
	}

	return files, nil
}

func (g googleClient) delete(name string) error {
	ctx := context.Background()

	err := g.bucketHandle.Object(path.Join(g.opts.Prefix, name)).Delete(ctx)
	if err != nil {
		if errors.Is(err, storagegcs.ErrObjectNotExist) {
			return storage.ErrNotExist
		}
		return errors.Wrap(err, "delete object")
	}

	return nil
}

func (g googleClient) copy(src, dst string) error {
	ctx := context.Background()

	srcObj := g.bucketHandle.Object(path.Join(g.opts.Prefix, src))
	dstObj := g.bucketHandle.Object(path.Join(g.opts.Prefix, dst))

	_, err := dstObj.CopierFrom(srcObj).Run(ctx)
	return err
}

func (g googleClient) getPartialObject(ctx context.Context, name string, start, length int64) (io.ReadCloser, error) {
	obj := g.bucketHandle.Object(path.Join(g.opts.Prefix, name))
	reader, err := obj.NewRangeReader(ctx, start, length)
	if err != nil {
		if errors.Is(err, storagegcs.ErrObjectNotExist) || isRangeNotSatisfiable(err) {
			return nil, io.EOF
		}

		return nil, storage.GetObjError{Err: err}
	}
	return reader, nil
}
