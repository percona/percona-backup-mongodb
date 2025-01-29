package s3

import (
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go/logging"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

const (
	// GCSEndpointURL is the endpoint url for Google Clound Strage service
	GCSEndpointURL        = "storage.googleapis.com"
	defaultPartSize int64 = 10 * 1024 * 1024 // 10Mb
	defaultS3Region       = "us-east-1"
)

//nolint:lll
type Config struct {
	Provider             string            `bson:"provider,omitempty" json:"provider,omitempty" yaml:"provider,omitempty"`
	Region               string            `bson:"region" json:"region" yaml:"region"`
	EndpointURL          string            `bson:"endpointUrl,omitempty" json:"endpointUrl" yaml:"endpointUrl,omitempty"`
	EndpointURLMap       map[string]string `bson:"endpointUrlMap,omitempty" json:"endpointUrlMap,omitempty" yaml:"endpointUrlMap,omitempty"`
	ForcePathStyle       *bool             `bson:"forcePathStyle,omitempty" json:"forcePathStyle,omitempty" yaml:"forcePathStyle,omitempty"`
	Bucket               string            `bson:"bucket" json:"bucket" yaml:"bucket"`
	Prefix               string            `bson:"prefix,omitempty" json:"prefix,omitempty" yaml:"prefix,omitempty"`
	Credentials          Credentials       `bson:"credentials" json:"-" yaml:"credentials"`
	ServerSideEncryption *AWSsse           `bson:"serverSideEncryption,omitempty" json:"serverSideEncryption,omitempty" yaml:"serverSideEncryption,omitempty"`
	UploadPartSize       int               `bson:"uploadPartSize,omitempty" json:"uploadPartSize,omitempty" yaml:"uploadPartSize,omitempty"`
	MaxUploadParts       int32             `bson:"maxUploadParts,omitempty" json:"maxUploadParts,omitempty" yaml:"maxUploadParts,omitempty"`
	StorageClass         string            `bson:"storageClass,omitempty" json:"storageClass,omitempty" yaml:"storageClass,omitempty"`

	// InsecureSkipTLSVerify disables client verification of the server's
	// certificate chain and host name
	InsecureSkipTLSVerify bool `bson:"insecureSkipTLSVerify" json:"insecureSkipTLSVerify" yaml:"insecureSkipTLSVerify"`

	// DebugLogLevels enables AWS SDK debug logging (sub)levels. Available options:
	// LogDebug, Signing, HTTPBody, RequestRetries, RequestErrors, EventStreamBody
	//
	// Any sub levels will enable LogDebug level accordingly to AWS SDK Go module behavior
	// https://pkg.go.dev/github.com/aws/aws-sdk-go@v1.40.7/aws#LogLevelType
	DebugLogLevels string `bson:"debugLogLevels,omitempty" json:"debugLogLevels,omitempty" yaml:"debugLogLevels,omitempty"`

	// Retryer is configuration for client.DefaultRetryer
	// https://pkg.go.dev/github.com/aws/aws-sdk-go/aws/client#DefaultRetryer
	Retryer *Retryer `bson:"retryer,omitempty" json:"retryer,omitempty" yaml:"retryer,omitempty"`
}

type Retryer struct {
	// Num max Retries is the number of max retries that will be performed.
	// https://pkg.go.dev/github.com/aws/aws-sdk-go/aws/client#DefaultRetryer.NumMaxRetries
	NumMaxRetries int `bson:"numMaxRetries" json:"numMaxRetries" yaml:"numMaxRetries"`

	// MinRetryDelay is the minimum retry delay after which retry will be performed.
	// https://pkg.go.dev/github.com/aws/aws-sdk-go/aws/client#DefaultRetryer.MinRetryDelay
	MinRetryDelay time.Duration `bson:"minRetryDelay" json:"minRetryDelay" yaml:"minRetryDelay"`

	// MaxRetryDelay is the maximum retry delay before which retry must be performed.
	// https://pkg.go.dev/github.com/aws/aws-sdk-go/aws/client#DefaultRetryer.MaxRetryDelay
	MaxRetryDelay time.Duration `bson:"maxRetryDelay" json:"maxRetryDelay" yaml:"maxRetryDelay"`
}

type SDKDebugLogLevel string

const (
	Signing              SDKDebugLogLevel = "Signing"
	Retries              SDKDebugLogLevel = "Retries"
	Request              SDKDebugLogLevel = "Request"
	RequestWithBody      SDKDebugLogLevel = "RequestWithBody"
	Response             SDKDebugLogLevel = "Response"
	ResponseWithBody     SDKDebugLogLevel = "ResponseWithBody"
	DeprecatedUsage      SDKDebugLogLevel = "DeprecatedUsage"
	RequestEventMessage  SDKDebugLogLevel = "RequestEventMessage"
	ResponseEventMessage SDKDebugLogLevel = "ResponseEventMessage"

	LogDebug        SDKDebugLogLevel = "LogDebug"
	HTTPBody        SDKDebugLogLevel = "HTTPBody"
	RequestRetries  SDKDebugLogLevel = "RequestRetries"
	RequestErrors   SDKDebugLogLevel = "RequestErrors"
	EventStreamBody SDKDebugLogLevel = "EventStreamBody"
)

type AWSsse struct {
	// Used to specify the SSE algorithm used when keys are managed by the server
	SseAlgorithm string `bson:"sseAlgorithm" json:"sseAlgorithm" yaml:"sseAlgorithm"`
	KmsKeyID     string `bson:"kmsKeyID" json:"kmsKeyID" yaml:"kmsKeyID"`
	// Used to specify SSE-C style encryption. For Amazon S3 SseCustomerAlgorithm must be 'AES256'
	// see https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html
	SseCustomerAlgorithm string `bson:"sseCustomerAlgorithm" json:"sseCustomerAlgorithm" yaml:"sseCustomerAlgorithm"`
	// If SseCustomerAlgorithm is set, this must be a base64 encoded key compatible with the algorithm
	// specified in the SseCustomerAlgorithm field.
	SseCustomerKey string `bson:"sseCustomerKey" json:"sseCustomerKey" yaml:"sseCustomerKey"`
}

func (cfg *Config) Clone() *Config {
	if cfg == nil {
		return nil
	}

	rv := *cfg
	rv.EndpointURLMap = maps.Clone(cfg.EndpointURLMap)
	if cfg.ForcePathStyle != nil {
		a := *cfg.ForcePathStyle
		rv.ForcePathStyle = &a
	}
	if cfg.ServerSideEncryption != nil {
		a := *cfg.ServerSideEncryption
		rv.ServerSideEncryption = &a
	}
	if cfg.Retryer != nil {
		a := *cfg.Retryer
		rv.Retryer = &a
	}

	return &rv
}

func (cfg *Config) Equal(other *Config) bool {
	if cfg == nil || other == nil {
		return cfg == other
	}

	if cfg.Region != other.Region {
		return false
	}
	if cfg.EndpointURL != other.EndpointURL {
		return false
	}
	if !maps.Equal(cfg.EndpointURLMap, other.EndpointURLMap) {
		return false
	}
	if cfg.Bucket != other.Bucket {
		return false
	}
	if cfg.Prefix != other.Prefix {
		return false
	}
	if cfg.StorageClass != other.StorageClass {
		return false
	}

	lhs, rhs := true, true
	if cfg.ForcePathStyle != nil {
		lhs = *cfg.ForcePathStyle
	}
	if other.ForcePathStyle != nil {
		rhs = *other.ForcePathStyle
	}
	if lhs != rhs {
		return false
	}

	// TODO: check only required fields
	if !reflect.DeepEqual(cfg.Credentials, other.Credentials) {
		return false
	}
	// TODO: check only required fields
	if !reflect.DeepEqual(cfg.ServerSideEncryption, other.ServerSideEncryption) {
		return false
	}

	return true
}

func (cfg *Config) Cast() error {
	if cfg.Region == "" {
		cfg.Region = defaultS3Region
	}
	if cfg.ForcePathStyle == nil {
		trueVal := true
		cfg.ForcePathStyle = &trueVal
	}
	if cfg.MaxUploadParts <= 0 {
		cfg.MaxUploadParts = manager.MaxUploadParts
	}
	if cfg.StorageClass == "" {
		cfg.StorageClass = string(types.StorageClassStandard)
	}

	if cfg.Retryer != nil {
		if cfg.Retryer.MinRetryDelay == 0 {
			// cfg.Retryer.MinRetryDelay = client.DefaultRetryerMinRetryDelay
			cfg.Retryer.MinRetryDelay = 30 * time.Millisecond
		}
		if cfg.Retryer.MaxRetryDelay == 0 {
			// cfg.Retryer.MaxRetryDelay = client.DefaultRetryerMaxRetryDelay
			cfg.Retryer.MaxRetryDelay = 300 * time.Second
		}
	}

	return nil
}

// resolveEndpointURL returns endpoint url based on provided
// EndpointURL or associated EndpointURLMap configuration fields.
// If specified EndpointURLMap overrides EndpointURL field.
func (cfg *Config) resolveEndpointURL(node string) string {
	ep := cfg.EndpointURL
	if epm, ok := cfg.EndpointURLMap[node]; ok {
		ep = epm
	}
	return ep
}

// SDKLogLevel returns AWS SDK log level value from comma-separated
// SDKDebugLogLevel values string. If the string does not contain a valid value,
// returns aws.LogOff.
//
// If the string is incorrect formatted, prints warnings to the io.Writer.
// Passing nil as the io.Writer will discard any warnings.
func SDKLogLevel(levels string, out io.Writer) aws.ClientLogMode {
	if out == nil {
		out = io.Discard
	}

	var logLevel aws.ClientLogMode

	for _, lvl := range strings.Split(levels, ",") {
		lvl = strings.TrimSpace(lvl)
		if lvl == "" {
			continue
		}

		l := toClientLogMode(lvl)
		if l == 0 {
			fmt.Fprintf(out, "Warning: S3 client debug log level: unsupported %q\n", lvl)
			continue
		}

		logLevel |= l
	}

	return logLevel
}

type Credentials struct {
	AccessKeyID     string `bson:"access-key-id" json:"access-key-id,omitempty" yaml:"access-key-id,omitempty"`
	SecretAccessKey string `bson:"secret-access-key" json:"secret-access-key,omitempty" yaml:"secret-access-key,omitempty"`
	SessionToken    string `bson:"session-token" json:"session-token,omitempty" yaml:"session-token,omitempty"`
	Vault           struct {
		Server string `bson:"server" json:"server,omitempty" yaml:"server"`
		Secret string `bson:"secret" json:"secret,omitempty" yaml:"secret"`
		Token  string `bson:"token" json:"token,omitempty" yaml:"token"`
	} `bson:"vault" json:"vault" yaml:"vault,omitempty"`
}

type S3 struct {
	opts  *Config
	node  string
	log   log.LogEvent
	s3cli *s3.Client

	d *Download // default downloader for small files
}

func New(opts *Config, node string, l log.LogEvent) (*S3, error) {
	if err := opts.Cast(); err != nil {
		return nil, errors.Wrap(err, "cast options")
	}

	if l == nil {
		l = log.DiscardEvent
	}

	s := &S3{
		opts: opts,
		log:  l,
		node: node,
	}

	cli, err := s.s3client()
	if err != nil {
		return nil, errors.Wrap(err, "AWS session")
	}
	s.s3cli = cli

	s.d = &Download{
		s3:       s,
		arenas:   []*arena{newArena(downloadChuckSizeDefault, downloadChuckSizeDefault)},
		spanSize: downloadChuckSizeDefault,
		cc:       1,
	}

	return s, nil
}

func (*S3) Type() storage.Type {
	return storage.S3
}

func (s *S3) Save(name string, data io.Reader, sizeb int64) error {
	cc := runtime.NumCPU() / 2
	if cc == 0 {
		cc = 1
	}

	putInput := &s3.PutObjectInput{
		Bucket:       aws.String(s.opts.Bucket),
		Key:          aws.String(path.Join(s.opts.Prefix, name)),
		Body:         data,
		StorageClass: types.StorageClass(s.opts.StorageClass),
	}

	sse := s.opts.ServerSideEncryption
	if sse != nil {
		if sse.SseAlgorithm == string(types.ServerSideEncryptionAes256) {
			putInput.ServerSideEncryption = types.ServerSideEncryptionAes256
		} else if sse.SseAlgorithm == string(types.ServerSideEncryptionAwsKms) {
			putInput.ServerSideEncryption = types.ServerSideEncryptionAwsKms
			putInput.SSEKMSKeyId = aws.String(sse.KmsKeyID)
		} else if sse.SseCustomerAlgorithm != "" {
			putInput.SSECustomerAlgorithm = aws.String(sse.SseCustomerAlgorithm)
			decodedKey, err := base64.StdEncoding.DecodeString(sse.SseCustomerKey)
			if err != nil {
				return errors.Wrap(err, "SseCustomerAlgorithm specified with invalid SseCustomerKey")
			}
			putInput.SSECustomerKey = aws.String(string(decodedKey))
			keyMD5 := md5.Sum(decodedKey)
			putInput.SSECustomerKeyMD5 = aws.String(base64.StdEncoding.EncodeToString(keyMD5[:]))
		}
	}

	// MaxUploadParts is 1e4 so with PartSize 10Mb the max allowed file size
	// would be ~ 97.6Gb. Hence if the file size is bigger we're enlarging PartSize
	// so PartSize * MaxUploadParts could fit the file.
	// If calculated PartSize is smaller than the default we leave the default.
	// If UploadPartSize option was set we use it instead of the default. Even
	// with the UploadPartSize set the calculated PartSize woulbe used if it's bigger.
	partSize := defaultPartSize
	if s.opts.UploadPartSize > 0 {
		if s.opts.UploadPartSize < int(manager.MinUploadPartSize) {
			s.opts.UploadPartSize = int(manager.MinUploadPartSize)
		}

		partSize = int64(s.opts.UploadPartSize)
	}
	if sizeb > 0 {
		ps := sizeb / int64(manager.MaxUploadParts) * 11 / 10 // add 10% just in case
		if ps > partSize {
			partSize = ps
		}
	}

	if s.log != nil {
		s.log.Debug("uploading %q [size hint: %v (%v); part size: %v (%v)]",
			name,
			sizeb,
			storage.PrettySize(sizeb),
			partSize,
			storage.PrettySize(partSize))
	}

	_, err := manager.NewUploader(s.s3cli, func(u *manager.Uploader) {
		u.MaxUploadParts = s.opts.MaxUploadParts
		u.PartSize = partSize      // 10MB part size
		u.LeavePartsOnError = true // Don't delete the parts if the upload fails.
		u.Concurrency = cc
	}).Upload(context.TODO(), putInput)

	return errors.Wrap(err, "upload to S3")
}

func (s *S3) List(prefix, suffix string) ([]storage.FileInfo, error) {
	ctx := context.TODO()

	prfx := path.Join(s.opts.Prefix, prefix)

	if prfx != "" && !strings.HasSuffix(prfx, "/") {
		prfx += "/"
	}

	lparams := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.opts.Bucket),
	}

	if prfx != "" {
		lparams.Prefix = aws.String(prfx)
	}

	var files []storage.FileInfo
	paginator := s3.NewListObjectsV2Paginator(s.s3cli, lparams)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "list objects pagination")
		}

		for _, obj := range page.Contents {
			f := aws.ToString(obj.Key)
			f = strings.TrimPrefix(f, prfx)
			if len(f) == 0 {
				continue
			}
			if f[0] == '/' {
				f = f[1:]
			}

			if strings.HasSuffix(f, suffix) {
				files = append(files, storage.FileInfo{
					Name: f,
					Size: *obj.Size,
				})
			}
		}
	}

	return files, nil
}

func (s *S3) Copy(src, dst string) error {
	copyOpts := &s3.CopyObjectInput{
		Bucket:     aws.String(s.opts.Bucket),
		CopySource: aws.String(path.Join(s.opts.Bucket, s.opts.Prefix, src)),
		Key:        aws.String(path.Join(s.opts.Prefix, dst)),
	}

	sse := s.opts.ServerSideEncryption
	if sse != nil {
		if sse.SseAlgorithm == string(types.ServerSideEncryptionAwsKms) {
			copyOpts.ServerSideEncryption = types.ServerSideEncryptionAwsKms
			copyOpts.SSEKMSKeyId = aws.String(sse.KmsKeyID)
		} else if sse.SseCustomerAlgorithm != "" {
			copyOpts.SSECustomerAlgorithm = aws.String(sse.SseCustomerAlgorithm)
			decodedKey, err := base64.StdEncoding.DecodeString(sse.SseCustomerKey)
			if err != nil {
				return errors.Wrap(err, "SseCustomerAlgorithm specified with invalid SseCustomerKey")
			}
			copyOpts.SSECustomerKey = aws.String(string(decodedKey))
			keyMD5 := md5.Sum(decodedKey)
			copyOpts.SSECustomerKeyMD5 = aws.String(base64.StdEncoding.EncodeToString(keyMD5[:]))

			copyOpts.CopySourceSSECustomerAlgorithm = copyOpts.SSECustomerAlgorithm
			copyOpts.CopySourceSSECustomerKey = copyOpts.SSECustomerKey
			copyOpts.CopySourceSSECustomerKeyMD5 = copyOpts.SSECustomerKeyMD5
		}
	}

	_, err := s.s3cli.CopyObject(context.TODO(), copyOpts)

	return err
}

func (s *S3) FileStat(name string) (storage.FileInfo, error) {
	inf := storage.FileInfo{}

	headOpts := &s3.HeadObjectInput{
		Bucket: aws.String(s.opts.Bucket),
		Key:    aws.String(path.Join(s.opts.Prefix, name)),
	}

	sse := s.opts.ServerSideEncryption
	if sse != nil && sse.SseCustomerAlgorithm != "" {
		headOpts.SSECustomerAlgorithm = aws.String(sse.SseCustomerAlgorithm)
		decodedKey, err := base64.StdEncoding.DecodeString(sse.SseCustomerKey)
		headOpts.SSECustomerKey = aws.String(string(decodedKey))
		if err != nil {
			return inf, errors.Wrap(err, "SseCustomerAlgorithm specified with invalid SseCustomerKey")
		}
		keyMD5 := md5.Sum(decodedKey)
		headOpts.SSECustomerKeyMD5 = aws.String(base64.StdEncoding.EncodeToString(keyMD5[:]))
	}

	h, err := s.s3cli.HeadObject(context.TODO(), headOpts)
	if err != nil {
		var noSuchKeyErr *types.NoSuchKey
		if errors.As(err, &noSuchKeyErr) {
			return inf, storage.ErrNotExist
		}

		var notFoundErr *types.NotFound
		if errors.As(err, &notFoundErr) {
			return inf, storage.ErrNotExist
		}
		return inf, errors.Wrap(err, "get S3 object header")
	}
	inf.Name = name
	inf.Size = *h.ContentLength

	if inf.Size == 0 {
		return inf, storage.ErrEmpty
	}

	if h.DeleteMarker != nil && *h.DeleteMarker {
		return inf, errors.New("file has delete marker")
	}

	return inf, nil
}

// Delete deletes given file.
// v1 - It returns storage.ErrNotExist if a file doesn't exist
// v2 - No longer returns an error if a file doesn't exist
func (s *S3) Delete(name string) error {
	_, err := s.s3cli.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.opts.Bucket),
		Key:    aws.String(path.Join(s.opts.Prefix, name)),
	})
	if err != nil {
		var noSuchKeyErr *types.NoSuchKey
		if errors.As(err, &noSuchKeyErr) {
			return storage.ErrNotExist
		}

		return errors.Wrapf(err, "delete '%s/%s' file from S3", s.opts.Bucket, name)
	}

	return nil
}

func (s *S3) buildLoadOptions() []func(*config.LoadOptions) error {
	httpClient := &http.Client{}
	if s.opts.InsecureSkipTLSVerify {
		httpClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
			},
		}
	}

	cfgOpts := []func(*config.LoadOptions) error{
		config.WithRegion(s.opts.Region),
		config.WithHTTPClient(httpClient),
		config.WithClientLogMode(toClientLogMode(s.opts.DebugLogLevels)),
	}

	endpointURL := s.opts.resolveEndpointURL(s.node)
	if endpointURL != "" {
		cfgOpts = append(cfgOpts, config.WithBaseEndpoint(endpointURL))
	}

	if s.log != nil {
		cfgOpts = append(cfgOpts, config.WithLogger(awsLogger{l: s.log}))
	}

	if s.opts.Retryer != nil {
		customRetryer := func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				// v2 MaxAttempts includes the first try
				o.MaxAttempts = s.opts.Retryer.NumMaxRetries + 1
				// TODO: determine Backoff based on MinRetryDelay and MaxRetryDelay
			})
		}
		cfgOpts = append(cfgOpts, config.WithRetryer(customRetryer))
	}

	if s.opts.Credentials.AccessKeyID != "" && s.opts.Credentials.SecretAccessKey != "" {
		staticProv := credentials.NewStaticCredentialsProvider(
			s.opts.Credentials.AccessKeyID,
			s.opts.Credentials.SecretAccessKey,
			s.opts.Credentials.SessionToken,
		)
		cfgOpts = append(cfgOpts, config.WithCredentialsProvider(staticProv))
	}

	return cfgOpts
}

func (s *S3) s3client() (*s3.Client, error) {
	ctx := context.TODO()

	cfgOpts := s.buildLoadOptions()

	cfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "load default config")
	}

	// If defined, IRSA-related credentials should have the priority over any potential role attached to the EC2.
	awsRoleARN := os.Getenv("AWS_ROLE_ARN")
	awsTokenFile := os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE")

	if awsRoleARN != "" && awsTokenFile != "" {
		stsClient := sts.NewFromConfig(cfg)
		webRole := stscreds.NewWebIdentityRoleProvider(
			stsClient, awsRoleARN, stscreds.IdentityTokenFile(awsTokenFile),
		)
		cfg.Credentials = aws.NewCredentialsCache(webRole)
	}

	if strings.Contains(s.opts.EndpointURL, "storage.googleapis.com") {
		cfg.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		cfg.HTTPClient = &http.Client{
			Transport: &RecalculateV4Signature{http.DefaultTransport, v4.NewSigner(), cfg},
		}
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = *s.opts.ForcePathStyle
	}), nil
}

type awsLogger struct {
	l log.LogEvent
}

func (a awsLogger) Logf(_ logging.Classification, _ string, xs ...interface{}) {
	if a.l == nil {
		return
	}

	if len(xs) == 0 {
		return
	}

	msg := "%v"
	for i := len(xs) - 1; i > 0; i-- {
		msg += " %v"
	}

	a.l.Debug(msg, xs...)
}

func toClientLogMode(levels string) aws.ClientLogMode {
	var mode aws.ClientLogMode
	items := strings.Split(levels, ",")

	for _, item := range items {
		flag := strings.TrimSpace(item)

		switch flag {
		case "Signing":
			// v1 had "LogDebugWithSigning"
			mode |= aws.LogSigning

		case "Retries":
			mode |= aws.LogRetries

		case "Request":
			mode |= aws.LogRequest

		case "RequestWithBody":
			mode |= aws.LogRequestWithBody

		case "Response":
			mode |= aws.LogResponse

		case "ResponseWithBody":
			mode |= aws.LogResponseWithBody

		case "DeprecatedUsage":
			mode |= aws.LogDeprecatedUsage

		case "RequestEventMessage":
			mode |= aws.LogRequestEventMessage

		case "ResponseEventMessage":
			mode |= aws.LogResponseEventMessage

		// Mapping deprecated flags from v1 for backwards compatibility
		case "LogDebug":
			// v1 had "LogDebug"
			mode |= aws.LogRequest | aws.LogResponse

		case "HTTPBody":
			// v1 had "LogDebugWithHTTPBody"
			mode |= aws.LogRequestWithBody | aws.LogResponseWithBody

		case "RequestRetries":
			// v1 had "LogDebugWithRequestRetries"
			mode |= aws.LogRetries

		case "RequestErrors":
			// v1 had "LogDebugWithRequestErrors"
			mode |= aws.LogResponse

		case "EventStreamBody":
			// v1 had "LogDebugWithEventStreamBody"
			mode |= aws.LogRequestWithBody | aws.LogResponseWithBody
		}
	}

	return mode
}

type RecalculateV4Signature struct {
	next   http.RoundTripper
	signer *v4.Signer
	cfg    aws.Config
}

func (lt *RecalculateV4Signature) RoundTrip(req *http.Request) (*http.Response, error) {
	val := req.Header.Get("Accept-Encoding")

	// delete the header so the header doesn't account for in the signature
	req.Header.Del("Accept-Encoding")

	// sign with the same date
	timeString := req.Header.Get("X-Amz-Date")
	timeDate, _ := time.Parse("20060102T150405Z", timeString)

	creds, _ := lt.cfg.Credentials.Retrieve(req.Context())
	err := lt.signer.SignHTTP(req.Context(), creds, req, v4.GetPayloadHash(req.Context()), "s3", lt.cfg.Region, timeDate)
	if err != nil {
		return nil, err
	}

	// Reset Accept-Encoding
	req.Header.Set("Accept-Encoding", val)

	// follows up the original round tripper
	return lt.next.RoundTrip(req)
}
