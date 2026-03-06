package oss

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	osscred "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/retry"
	"github.com/aliyun/credentials-go/credentials/providers"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

var _ storage.Storage = &OSS{}

const (
	ServerSideEncryptionAes256 = "AES256"
	ServerSideEncryptionKMS    = "KMS"
	ServerSideEncryptionSM4    = "SM4"
)

func New(cfg *Config, node string, l log.LogEvent) (storage.Storage, error) {
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

	return storage.NewSplitMergeMW(o, cfg.GetMaxObjSizeGB()), nil
}

func configureClient(config *Config) (*oss.Client, error) {
	if config == nil {
		return nil, fmt.Errorf("config is nil")
	}

	if config.Region == "" || config.Bucket == "" ||
		config.Credentials.AccessKeyID == "" ||
		config.Credentials.AccessKeySecret == "" {
		return nil, fmt.Errorf("missing required OSS config: %+v", config)
	}

	cred, err := newCred(config)
	if err != nil {
		return nil, fmt.Errorf("create credentials: %w", err)
	}

	ossConfig := oss.LoadDefaultConfig().
		WithRegion(config.Region).
		WithCredentialsProvider(cred).
		WithSignatureVersion(oss.SignatureVersionV4).
		WithRetryMaxAttempts(config.Retryer.MaxAttempts).
		WithRetryer(retry.NewStandard(func(ro *retry.RetryOptions) {
			ro.MaxAttempts = config.Retryer.MaxAttempts
			ro.MaxBackoff = config.Retryer.MaxBackoff
			ro.BaseDelay = config.Retryer.BaseDelay
		})).
		WithConnectTimeout(config.ConnectTimeout)

	if config.EndpointURL != "" {
		ossConfig = ossConfig.WithEndpoint(config.EndpointURL)
	}

	return oss.NewClient(ossConfig), nil
}

func newCred(config *Config) (*cred, error) {
	var credentialsProvider providers.CredentialsProvider
	var err error

	if config.Credentials.AccessKeyID == "" || config.Credentials.AccessKeySecret == "" {
		return nil, fmt.Errorf("access key ID and secret are required")
	}

	if config.Credentials.SecurityToken != "" {
		credentialsProvider, err = providers.NewStaticSTSCredentialsProviderBuilder().
			WithAccessKeyId(string(config.Credentials.AccessKeyID)).
			WithAccessKeySecret(string(config.Credentials.AccessKeySecret)).
			WithSecurityToken(string(config.Credentials.SecurityToken)).
			Build()
	} else {
		credentialsProvider, err = providers.NewStaticAKCredentialsProviderBuilder().
			WithAccessKeyId(string(config.Credentials.AccessKeyID)).
			WithAccessKeySecret(string(config.Credentials.AccessKeySecret)).
			Build()
	}
	if err != nil {
		return nil, fmt.Errorf("credentials provider: %w", err)
	}

	if config.Credentials.RoleARN != "" {
		internalProvider := credentialsProvider
		credentialsProvider, err = providers.NewRAMRoleARNCredentialsProviderBuilder().
			WithCredentialsProvider(internalProvider).
			WithRoleArn(string(config.Credentials.RoleARN)).
			WithRoleSessionName(string(config.Credentials.SessionName)).
			WithDurationSeconds(defaultSessionDurationSeconds).
			Build()
		if err != nil {
			return nil, fmt.Errorf("ram role credential provider: %w", err)
		}
	}

	return &cred{
		provider: credentialsProvider,
	}, nil
}

type cred struct {
	provider providers.CredentialsProvider
}

func (c *cred) GetCredentials(ctx context.Context) (osscred.Credentials, error) {
	cc, err := c.provider.GetCredentials()
	if err != nil {
		return osscred.Credentials{}, err
	}

	return osscred.Credentials{
		AccessKeyID:     cc.AccessKeyId,
		AccessKeySecret: cc.AccessKeySecret,
		SecurityToken:   cc.SecurityToken,
	}, nil
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

	req := &oss.PutObjectRequest{
		Bucket: oss.Ptr(o.cfg.Bucket),
		Key:    oss.Ptr(path.Join(o.cfg.Prefix, name)),
	}

	if o.cfg.ServerSideEncryption != nil {
		sse := o.cfg.ServerSideEncryption
		switch sse.EncryptionMethod {
		case ServerSideEncryptionSM4:
			req.ServerSideEncryption = oss.Ptr(ServerSideEncryptionSM4)
		case ServerSideEncryptionKMS:
			req.ServerSideEncryption = oss.Ptr(ServerSideEncryptionKMS)
			req.ServerSideDataEncryption = oss.Ptr(sse.EncryptionAlgorithm)
			req.ServerSideEncryptionKeyId = oss.Ptr(string(sse.EncryptionKeyID))
		default:
			req.ServerSideEncryption = oss.Ptr(ServerSideEncryptionAes256)
		}
	}

	partSize := storage.ComputePartSize(
		opts.Size,
		oss.DefaultUploadPartSize,
		oss.MinPartSize,
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

	uploader := oss.NewUploader(o.ossCli, func(uo *oss.UploaderOptions) {
		uo.PartSize = partSize
	})
	_, err := uploader.UploadFrom(context.Background(), req, data)

	return errors.Wrap(err, "put object")
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

	req := &oss.HeadObjectRequest{
		Bucket: oss.Ptr(o.cfg.Bucket),
		Key:    oss.Ptr(path.Join(o.cfg.Prefix, name)),
	}

	res, err := o.ossCli.HeadObject(context.Background(), req)
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
	if _, err := o.FileStat(name); err == storage.ErrNotExist {
		return err
	}

	key := path.Join(o.cfg.Prefix, name)
	_, err := o.ossCli.DeleteObject(context.Background(), &oss.DeleteObjectRequest{
		Bucket: oss.Ptr(o.cfg.Bucket),
		Key:    oss.Ptr(key),
	})
	if err != nil {
		return errors.Wrapf(err, "delete %s/%s file from OSS", o.cfg.Bucket, key)
	}
	return nil
}

// Copy makes a copy of the src object/file under dst name
func (o *OSS) Copy(src, dst string) error {
	req := &oss.CopyObjectRequest{
		Bucket:       oss.Ptr(o.cfg.Bucket),
		Key:          oss.Ptr(path.Join(o.cfg.Prefix, dst)),
		SourceBucket: oss.Ptr(o.cfg.Bucket),
		SourceKey:    oss.Ptr(path.Join(o.cfg.Prefix, src)),
	}

	if o.cfg.ServerSideEncryption != nil {
		sse := o.cfg.ServerSideEncryption
		switch sse.EncryptionMethod {
		case ServerSideEncryptionSM4:
			req.ServerSideEncryption = oss.Ptr(ServerSideEncryptionSM4)
		case ServerSideEncryptionKMS:
			req.ServerSideEncryption = oss.Ptr(ServerSideEncryptionKMS)
			req.ServerSideDataEncryption = oss.Ptr(sse.EncryptionAlgorithm)
			req.ServerSideEncryptionKeyId = oss.Ptr(string(sse.EncryptionKeyID))
		default:
			req.ServerSideEncryption = oss.Ptr(ServerSideEncryptionAes256)
		}
	}
	copier := oss.NewCopier(o.ossCli, func(co *oss.CopierOptions) {})
	_, err := copier.Copy(context.Background(), req)
	return errors.Wrap(err, "copy object")
}

func (o *OSS) DownloadStat() storage.DownloadStat {
	return storage.DownloadStat{}
}
