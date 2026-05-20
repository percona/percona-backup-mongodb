package oci

import (
	"context"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/oracle/oci-go-sdk/v65/objectstorage/transfer"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

var _ storage.Storage = (*OCI)(nil)

const (
	defaultCopyWorkRequestPollDelay = 2 * time.Second
)

func New(cfg *Config, node string, l log.LogEvent) (storage.Storage, error) {
	o, err := newOCI(cfg, node, l)
	if err != nil {
		return nil, err
	}
	o.d = newDownload(1, storage.DownloadChuckSizeDefault, storage.DownloadChuckSizeDefault)

	return storage.NewSplitMergeMW(o, cfg.GetMaxObjSizeGB()), nil
}

func NewWithDownloader(
	cfg *Config,
	node string,
	l log.LogEvent,
	cc, bufSizeMb, spanSizeMb int,
) (storage.Storage, error) {
	o, err := newOCI(cfg, node, l)
	if err != nil {
		return nil, err
	}

	arenaSize, spanSize, cc := storage.DownloadOpts(cc, bufSizeMb, spanSizeMb)
	o.log.Debug("download max buf %d (arena %d, span %d, concurrency %d)", arenaSize*cc, arenaSize, spanSize, cc)
	o.d = newDownload(cc, arenaSize, spanSize)

	return storage.NewSplitMergeMW(o, cfg.GetMaxObjSizeGB()), nil
}

func newOCI(cfg *Config, node string, l log.LogEvent) (*OCI, error) {
	if err := cfg.Cast(); err != nil {
		return nil, errors.Wrap(err, "set defaults")
	}
	if l == nil {
		l = log.DiscardEvent
	}
	client, retryPolicy, err := configureClient(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "configure client")
	}

	o := &OCI{
		cfg:         cfg,
		node:        node,
		log:         l,
		client:      client,
		retryPolicy: retryPolicy,
	}

	return o, nil
}

func configureClient(cfg *Config) (*objectstorage.ObjectStorageClient, *common.RetryPolicy, error) {
	if cfg == nil {
		return nil, nil, errors.New("config is nil")
	}
	if cfg.Region == "" {
		return nil, nil, errors.New("region is required")
	}
	if cfg.Namespace == "" {
		return nil, nil, errors.New("namespace is required")
	}
	if cfg.Bucket == "" {
		return nil, nil, errors.New("bucket is required")
	}
	if cfg.Credentials.Tenancy == "" {
		return nil, nil, errors.New("credentials.tenancy is required")
	}
	if cfg.Credentials.User == "" {
		return nil, nil, errors.New("credentials.user is required")
	}
	if cfg.Credentials.Fingerprint == "" {
		return nil, nil, errors.New("credentials.fingerprint is required")
	}
	if cfg.Credentials.PrivateKey == "" {
		return nil, nil, errors.New("credentials.privateKey is required")
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
		return nil, nil, err
	}
	retryPolicy := newRetryPolicy(cfg.Retryer)
	client.SetCustomClientConfiguration(common.CustomClientConfiguration{
		RetryPolicy: retryPolicy,
	})
	// Match OCI transfer manager behavior: use a no-timeout client for large uploads.
	client.HTTPClient = &http.Client{}

	return &client, retryPolicy, nil
}

func newRetryPolicy(cfg *Retryer) *common.RetryPolicy {
	r := retryerWithDefaults(cfg)

	// Use OCI's standard retry rules but disable the SDK's extra
	// eventual-consistency retry window so failed storage calls don't wait longer
	// than the configured attempt count and backoff cap imply.
	nonEventuallyConsistent := common.NewRetryPolicyWithOptions(
		common.ReplaceWithValuesFromRetryPolicy(common.DefaultRetryPolicyWithoutEventualConsistency()),
		common.WithMaximumNumberAttempts(uint(r.MaxAttempts)),
		common.WithExponentialBackoff(r.MaxBackoff, retryBackoffBase),
	)

	return &nonEventuallyConsistent
}

type OCI struct {
	cfg         *Config
	node        string
	log         log.LogEvent
	client      *objectstorage.ObjectStorageClient
	retryPolicy *common.RetryPolicy
	d           *Download
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
		minUploadPartSize,
		int64(maxUploadParts),
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
			NumberOfGoroutines:    common.Int(o.cfg.UploadConcurrency),
			ObjectStorageClient:   o.client,
			// Override transfer manager's default, which retries any non-2xx response.
			RequestMetadata: o.requestMetadataWithRetryPolicy(),
		},
		StreamReader: data,
	})

	return errors.Wrap(err, "upload stream")
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
	ctx := context.Background()

	res, err := o.client.CopyObject(ctx, o.copyObjectRequest(src, dst))
	if err != nil {
		if isNotFound(err) {
			return storage.ErrNotExist
		}
		return errors.Wrapf(err, "copy %q/%q to %q file in OCI", o.cfg.Bucket, src, dst)
	}
	if res.OpcWorkRequestId == nil || *res.OpcWorkRequestId == "" {
		return errors.Errorf("copy %q/%q to %q file in OCI: work request id is empty", o.cfg.Bucket, src, dst)
	}

	if err := o.waitCopyWorkRequest(ctx, *res.OpcWorkRequestId); err != nil {
		return errors.Wrapf(err, "copy %q/%q to %q file in OCI", o.cfg.Bucket, src, dst)
	}

	return nil
}

func (o *OCI) requestMetadataWithRetryPolicy() common.RequestMetadata {
	return common.RequestMetadata{RetryPolicy: o.retryPolicy}
}

func (o *OCI) copyObjectRequest(src, dst string) objectstorage.CopyObjectRequest {
	return objectstorage.CopyObjectRequest{
		NamespaceName: common.String(o.cfg.Namespace),
		BucketName:    common.String(o.cfg.Bucket),
		CopyObjectDetails: objectstorage.CopyObjectDetails{
			SourceObjectName:      common.String(o.key(src)),
			DestinationRegion:     common.String(o.cfg.Region),
			DestinationNamespace:  common.String(o.cfg.Namespace),
			DestinationBucket:     common.String(o.cfg.Bucket),
			DestinationObjectName: common.String(o.key(dst)),
		},
	}
}

func (o *OCI) waitCopyWorkRequest(ctx context.Context, id string) error {
	for {
		res, err := o.client.GetWorkRequest(ctx, objectstorage.GetWorkRequestRequest{
			WorkRequestId: common.String(id),
		})
		if err != nil {
			return errors.Wrap(err, "get copy work request")
		}

		switch res.Status {
		case objectstorage.WorkRequestStatusCompleted:
			return nil
		case objectstorage.WorkRequestStatusFailed, objectstorage.WorkRequestStatusCanceled:
			return o.copyWorkRequestError(ctx, id, res.Status)
		case objectstorage.WorkRequestStatusAccepted,
			objectstorage.WorkRequestStatusInProgress,
			objectstorage.WorkRequestStatusCanceling:
		default:
			return errors.Errorf("copy object work request %s has unexpected status %q", id, res.Status)
		}

		poll := time.After(copyWorkRequestPollDelay(res.RetryAfter))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll:
		}
	}
}

func (o *OCI) copyWorkRequestError(ctx context.Context, id string, status objectstorage.WorkRequestStatusEnum) error {
	copyErr := &copyWorkRequestError{id: id, status: status}
	var page *string
	for {
		res, err := o.client.ListWorkRequestErrors(ctx, objectstorage.ListWorkRequestErrorsRequest{
			WorkRequestId: common.String(id),
			Page:          page,
		})
		if err != nil {
			copyErr.listErr = err
			return copyErr
		}

		copyErr.details = append(copyErr.details, res.Items...)
		page = res.OpcNextPage
		if page == nil {
			break
		}
	}

	return copyErr
}

func copyWorkRequestPollDelay(retryAfter *float32) time.Duration {
	if retryAfter != nil && *retryAfter > 0 {
		return time.Duration(float64(*retryAfter) * float64(time.Second))
	}

	return defaultCopyWorkRequestPollDelay
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

type copyWorkRequestError struct {
	id      string
	status  objectstorage.WorkRequestStatusEnum
	details []objectstorage.WorkRequestError
	listErr error
}

func (e *copyWorkRequestError) Error() string {
	msg := "copy object work request " + e.id + " finished with status " + string(e.status)
	details := e.detailMessages()

	if len(details) > 0 {
		msg += ": " + strings.Join(details, "; ")
		if e.listErr != nil {
			msg += "; failed to list complete error details: " + e.listErr.Error()
		}
		return msg
	}

	if e.listErr != nil {
		msg += "; failed to list error details: " + e.listErr.Error()
		return msg
	}

	msg += "; no error details returned"
	return msg
}

func (e *copyWorkRequestError) detailMessages() []string {
	if len(e.details) == 0 {
		return nil
	}

	messages := make([]string, 0, len(e.details))
	for _, item := range e.details {
		switch {
		case item.Code != nil && item.Message != nil:
			messages = append(messages, *item.Code+": "+*item.Message)
		case item.Message != nil:
			messages = append(messages, *item.Message)
		case item.Code != nil:
			messages = append(messages, *item.Code)
		default:
			messages = append(messages, "unknown work request error")
		}
	}

	return messages
}

func (e *copyWorkRequestError) Unwrap() error {
	return e.listErr
}
