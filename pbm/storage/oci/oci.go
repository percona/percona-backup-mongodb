package oci

import (
	"context"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/oracle/oci-go-sdk/v65/objectstorage/transfer"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

var _ storage.Storage = (*OCI)(nil)

func New(cfg *Config, node string, l log.LogEvent) (storage.Storage, error) {
	if err := cfg.Cast(); err != nil {
		return nil, errors.Wrap(err, "set defaults")
	}
	if l == nil {
		l = log.DiscardEvent
	}
	client, err := configureClient(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "configure client")
	}

	o := &OCI{
		cfg:    cfg,
		node:   node,
		log:    l,
		client: client,
	}

	return storage.NewSplitMergeMW(o, cfg.GetMaxObjSizeGB()), nil
}

func configureClient(cfg *Config) (*objectstorage.ObjectStorageClient, error) {
	if cfg == nil {
		return nil, errors.New("config is nil")
	}
	if cfg.Region == "" {
		return nil, errors.New("region is required")
	}
	if cfg.Namespace == "" {
		return nil, errors.New("namespace is required")
	}
	if cfg.Bucket == "" {
		return nil, errors.New("bucket is required")
	}
	if cfg.Credentials.Tenancy == "" {
		return nil, errors.New("credentials.tenancy is required")
	}
	if cfg.Credentials.User == "" {
		return nil, errors.New("credentials.user is required")
	}
	if cfg.Credentials.Fingerprint == "" {
		return nil, errors.New("credentials.fingerprint is required")
	}
	if cfg.Credentials.PrivateKey == "" {
		return nil, errors.New("credentials.privateKey is required")
	}

	var passphrase *string
	if cfg.Credentials.PrivateKeyPassphrase != "" {
		passphrase = common.String(string(cfg.Credentials.PrivateKeyPassphrase))
	}
	provider := common.NewRawConfigurationProvider(
		string(cfg.Credentials.Tenancy),
		string(cfg.Credentials.User),
		cfg.Region,
		string(cfg.Credentials.Fingerprint),
		string(cfg.Credentials.PrivateKey),
		passphrase,
	)

	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	if err != nil {
		return nil, err
	}
	// Match OCI transfer manager behavior: use a no-timeout client for large uploads.
	client.HTTPClient = &http.Client{}

	return &client, nil
}

type OCI struct {
	cfg    *Config
	node   string
	log    log.LogEvent
	client *objectstorage.ObjectStorageClient
}

func (*OCI) Type() storage.Type {
	return storage.OCI
}

func (o *OCI) Save(name string, data io.Reader, options ...storage.Option) error {
	opts := storage.GetDefaultOpts()
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return errors.Wrap(err, "processing options for save")
		}
	}

	partSize := storage.ComputePartSize(
		opts.Size,
		defaultUploadPartSize,
		defaultUploadPartSize,
		int64(o.cfg.MaxUploadParts),
		o.cfg.UploadPartSize,
	)

	if o.log != nil && opts.UseLogger {
		o.log.Debug("uploading %q [size hint: %v (%v); part size: %v (%v)]",
			name,
			opts.Size,
			storage.PrettySize(opts.Size),
			partSize,
			storage.PrettySize(partSize))
	}

	_, err := transfer.NewUploadManager().UploadStream(context.Background(), transfer.UploadStreamRequest{
		UploadRequest: transfer.UploadRequest{
			NamespaceName:         common.String(o.cfg.Namespace),
			BucketName:            common.String(o.cfg.Bucket),
			ObjectName:            common.String(o.key(name)),
			PartSize:              common.Int64(partSize),
			AllowMultipartUploads: common.Bool(true),
			AllowParrallelUploads: common.Bool(true),
			ObjectStorageClient:   o.client,
		},
		StreamReader: data,
	})

	return errors.Wrap(err, "upload stream")
}

func (o *OCI) SourceReader(name string) (io.ReadCloser, error) {
	res, err := o.client.GetObject(context.Background(), objectstorage.GetObjectRequest{
		NamespaceName: common.String(o.cfg.Namespace),
		BucketName:    common.String(o.cfg.Bucket),
		ObjectName:    common.String(o.key(name)),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, storage.ErrNotExist
		}
		return nil, errors.Wrap(err, "get object")
	}

	return res.Content, nil
}

func (o *OCI) FileStat(name string) (storage.FileInfo, error) {
	inf := storage.FileInfo{Name: name}
	res, err := o.client.HeadObject(context.Background(), objectstorage.HeadObjectRequest{
		NamespaceName: common.String(o.cfg.Namespace),
		BucketName:    common.String(o.cfg.Bucket),
		ObjectName:    common.String(o.key(name)),
	})
	if err != nil {
		if isNotFound(err) {
			return inf, storage.ErrNotExist
		}
		return inf, errors.Wrap(err, "head object")
	}

	if res.ContentLength != nil {
		inf.Size = *res.ContentLength
	}
	if inf.Size == 0 {
		return inf, storage.ErrEmpty
	}

	return inf, nil
}

func (o *OCI) List(prefix, suffix string) ([]storage.FileInfo, error) {
	prfx := path.Join(o.cfg.Prefix, prefix)
	if prfx != "" && !strings.HasSuffix(prfx, "/") {
		prfx += "/"
	}

	var files []storage.FileInfo
	var start *string
	for {
		res, err := o.client.ListObjects(context.Background(), objectstorage.ListObjectsRequest{
			NamespaceName: common.String(o.cfg.Namespace),
			BucketName:    common.String(o.cfg.Bucket),
			Prefix:        common.String(prfx),
			Start:         start,
			Fields:        common.String("name,size"),
		})
		if err != nil {
			return nil, errors.Wrap(err, "list objects")
		}

		for _, obj := range res.Objects {
			if obj.Name == nil {
				continue
			}
			f := strings.TrimPrefix(*obj.Name, prfx)
			if f == "" {
				continue
			}
			if f[0] == '/' {
				f = f[1:]
			}
			if !strings.HasSuffix(f, suffix) {
				continue
			}

			var size int64
			if obj.Size != nil {
				size = *obj.Size
			}
			files = append(files, storage.FileInfo{Name: f, Size: size})
		}

		if res.NextStartWith == nil {
			break
		}
		start = res.NextStartWith
	}

	return files, nil
}

func (o *OCI) Delete(name string) error {
	if _, err := o.FileStat(name); errors.Is(err, storage.ErrNotExist) {
		return err
	}

	_, err := o.client.DeleteObject(context.Background(), objectstorage.DeleteObjectRequest{
		NamespaceName: common.String(o.cfg.Namespace),
		BucketName:    common.String(o.cfg.Bucket),
		ObjectName:    common.String(o.key(name)),
	})
	if err != nil {
		if isNotFound(err) {
			return storage.ErrNotExist
		}
		return errors.Wrap(err, "delete object")
	}

	return nil
}

func (o *OCI) Copy(src, dst string) error {
	stat, err := o.FileStat(src)
	if err != nil {
		return errors.Wrap(err, "get source stat")
	}
	r, err := o.SourceReader(src)
	if err != nil {
		return errors.Wrap(err, "read source")
	}
	defer r.Close()

	return o.Save(dst, r, storage.Size(stat.Size))
}

func (o *OCI) DownloadStat() storage.DownloadStat {
	return storage.DownloadStat{}
}

func (o *OCI) key(name string) string {
	return path.Join(o.cfg.Prefix, name)
}

func isNotFound(err error) bool {
	if se, ok := common.IsServiceError(err); ok {
		return se.GetHTTPStatusCode() == http.StatusNotFound
	}
	return false
}
