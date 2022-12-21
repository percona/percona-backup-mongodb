package s3

import (
	"bytes"
	"container/heap"
	"crypto/md5"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/encrypt"
	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

const (
	// GCSEndpointURL is the endpoint url for Google Clound Strage service
	GCSEndpointURL = "storage.googleapis.com"

	defaultS3Region = "us-east-1"

	downloadChuckSize = 100 << 20 // 10Mb
	downloadRetries   = 10
)

type Conf struct {
	Provider             S3Provider  `bson:"provider,omitempty" json:"provider,omitempty" yaml:"provider,omitempty"`
	Region               string      `bson:"region" json:"region" yaml:"region"`
	EndpointURL          string      `bson:"endpointUrl,omitempty" json:"endpointUrl" yaml:"endpointUrl,omitempty"`
	Bucket               string      `bson:"bucket" json:"bucket" yaml:"bucket"`
	Prefix               string      `bson:"prefix,omitempty" json:"prefix,omitempty" yaml:"prefix,omitempty"`
	Credentials          Credentials `bson:"credentials" json:"-" yaml:"credentials"`
	ServerSideEncryption *AWSsse     `bson:"serverSideEncryption,omitempty" json:"serverSideEncryption,omitempty" yaml:"serverSideEncryption,omitempty"`
	UploadPartSize       int         `bson:"uploadPartSize,omitempty" json:"uploadPartSize,omitempty" yaml:"uploadPartSize,omitempty"`
	MaxUploadParts       int         `bson:"maxUploadParts,omitempty" json:"maxUploadParts,omitempty" yaml:"maxUploadParts,omitempty"`
	StorageClass         string      `bson:"storageClass,omitempty" json:"storageClass,omitempty" yaml:"storageClass,omitempty"`

	// NumDownloadWorkers sets the num of goroutine would be requesting chunks
	// during the download. By default, it's set to GOMAXPROCS. Setting this
	// option too high may result in performance degradation. As routines
	// might prefetch too many chunks (HTTPBody). Although we can prefetch
	// chunks concurrently the data should be written sequentially. And while
	// chunks will be  waiting to be read and sent to the destination the
	// `HTTPClient.Timeout` will run out and the chunk should be prefetched again.
	NumDownloadWorkers int `bson:"numDownloadWorkers" json:"numDownloadWorkers,omitempty" yaml:"numDownloadWorkers,omitempty"`

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
	LogDebug        SDKDebugLogLevel = "LogDebug"
	Signing         SDKDebugLogLevel = "Signing"
	HTTPBody        SDKDebugLogLevel = "HTTPBody"
	RequestRetries  SDKDebugLogLevel = "RequestRetries"
	RequestErrors   SDKDebugLogLevel = "RequestErrors"
	EventStreamBody SDKDebugLogLevel = "EventStreamBody"
)

// SDKLogLevel returns the appropriate AWS SDK debug logging level. If the level
// is not recognized, returns aws.LogLevelType(0)
func (l SDKDebugLogLevel) SDKLogLevel() aws.LogLevelType {
	switch l {
	case LogDebug:
		return aws.LogDebug
	case Signing:
		return aws.LogDebugWithSigning
	case HTTPBody:
		return aws.LogDebugWithHTTPBody
	case RequestRetries:
		return aws.LogDebugWithRequestRetries
	case RequestErrors:
		return aws.LogDebugWithRequestErrors
	case EventStreamBody:
		return aws.LogDebugWithEventStreamBody
	}

	return aws.LogLevelType(0)
}

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

func (c *Conf) Cast() error {
	if c.Region == "" {
		c.Region = defaultS3Region
	}
	if c.Provider == S3ProviderUndef {
		c.Provider = S3ProviderAWS
		if c.EndpointURL != "" {
			eu, err := url.Parse(c.EndpointURL)
			if err != nil {
				return errors.Wrap(err, "parse EndpointURL")
			}
			if eu.Host == GCSEndpointURL {
				c.Provider = S3ProviderGCS
			}
		}
	}
	if c.MaxUploadParts <= 0 {
		c.MaxUploadParts = s3manager.MaxUploadParts
	}
	if c.StorageClass == "" {
		c.StorageClass = s3.StorageClassStandard
	}

	if c.Retryer != nil {
		if c.Retryer.MinRetryDelay == 0 {
			c.Retryer.MinRetryDelay = client.DefaultRetryerMinRetryDelay
		}
		if c.Retryer.MaxRetryDelay == 0 {
			c.Retryer.MaxRetryDelay = client.DefaultRetryerMaxRetryDelay
		}
	}

	return nil
}

// SDKLogLevel returns AWS SDK log level value from comma-separated
// SDKDebugLogLevel values string. If the string does not contain a valid value,
// returns aws.LogOff.
//
// If the string is incorrect formatted, prints warnings to the io.Writer.
// Passing nil as the io.Writer will discard any warnings.
func SDKLogLevel(levels string, out io.Writer) aws.LogLevelType {
	if out == nil {
		out = io.Discard
	}

	var logLevel aws.LogLevelType

	for _, lvl := range strings.Split(levels, ",") {
		lvl = strings.TrimSpace(lvl)
		if lvl == "" {
			continue
		}

		l := SDKDebugLogLevel(lvl).SDKLogLevel()
		if l == 0 {
			fmt.Fprintf(out, "Warning: S3 client debug log level: unsupported %q\n", lvl)
			continue
		}

		logLevel |= l
	}

	if logLevel == 0 {
		logLevel = aws.LogOff
	}

	return logLevel
}

type Credentials struct {
	AccessKeyID     string `bson:"access-key-id" json:"access-key-id,omitempty" yaml:"access-key-id,omitempty"`
	SecretAccessKey string `bson:"secret-access-key" json:"secret-access-key,omitempty" yaml:"secret-access-key,omitempty"`
	Vault           struct {
		Server string `bson:"server" json:"server,omitempty" yaml:"server"`
		Secret string `bson:"secret" json:"secret,omitempty" yaml:"secret"`
		Token  string `bson:"token" json:"token,omitempty" yaml:"token"`
	} `bson:"vault" json:"vault" yaml:"vault,omitempty"`
}

type S3Provider string

const (
	S3ProviderUndef S3Provider = ""
	S3ProviderAWS   S3Provider = "aws"
	S3ProviderGCS   S3Provider = "gcs"
)

type S3 struct {
	opts Conf
	log  *log.Event
	s3s  *s3.S3
}

func New(opts Conf, l *log.Event) (*S3, error) {
	err := opts.Cast()
	if err != nil {
		return nil, errors.Wrap(err, "cast options")
	}

	s := &S3{
		opts: opts,
		log:  l,
	}

	s.s3s, err = s.s3session()
	if err != nil {
		return nil, errors.Wrap(err, "AWS session")
	}

	return s, nil
}

const defaultPartSize int64 = 10 * 1024 * 1024 // 10Mb

func (*S3) Type() storage.Type {
	return storage.S3
}

func (s *S3) Save(name string, data io.Reader, sizeb int64) error {
	switch s.opts.Provider {
	default:
		awsSession, err := s.session()
		if err != nil {
			return errors.Wrap(err, "create AWS session")
		}
		cc := runtime.NumCPU() / 2
		if cc == 0 {
			cc = 1
		}

		uplInput := &s3manager.UploadInput{
			Bucket:       aws.String(s.opts.Bucket),
			Key:          aws.String(path.Join(s.opts.Prefix, name)),
			Body:         data,
			StorageClass: &s.opts.StorageClass,
		}

		sse := s.opts.ServerSideEncryption
		if sse != nil {
			if sse.SseAlgorithm == s3.ServerSideEncryptionAwsKms {
				uplInput.ServerSideEncryption = aws.String(sse.SseAlgorithm)
				uplInput.SSEKMSKeyId = aws.String(sse.KmsKeyID)
			} else if sse.SseCustomerAlgorithm != "" {
				uplInput.SSECustomerAlgorithm = aws.String(sse.SseCustomerAlgorithm)
				decodedKey, err := base64.StdEncoding.DecodeString(sse.SseCustomerKey)
				uplInput.SSECustomerKey = aws.String(string(decodedKey[:]))
				if err != nil {
					return errors.Wrap(err, "SseCustomerAlgorithm specified with invalid SseCustomerKey")
				}
				keyMD5 := md5.Sum(decodedKey[:])
				uplInput.SSECustomerKeyMD5 = aws.String(base64.StdEncoding.EncodeToString(keyMD5[:]))
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
			if s.opts.UploadPartSize < int(s3manager.MinUploadPartSize) {
				s.opts.UploadPartSize = int(s3manager.MinUploadPartSize)
			}

			partSize = int64(s.opts.UploadPartSize)
		}
		if sizeb > 0 {
			ps := sizeb / s3manager.MaxUploadParts * 11 / 10 // add 10% just in case
			if ps > partSize {
				partSize = ps
			}
		}

		_, err = s3manager.NewUploader(awsSession, func(u *s3manager.Uploader) {
			u.MaxUploadParts = s.opts.MaxUploadParts
			u.PartSize = partSize      // 10MB part size
			u.LeavePartsOnError = true // Don't delete the parts if the upload fails.
			u.Concurrency = cc

			u.RequestOptions = append(u.RequestOptions, func(r *request.Request) {
				if s.opts.Retryer != nil {
					r.Retryer = client.DefaultRetryer{
						NumMaxRetries: s.opts.Retryer.NumMaxRetries,
						MinRetryDelay: s.opts.Retryer.MinRetryDelay,
						MaxRetryDelay: s.opts.Retryer.MaxRetryDelay,
					}
				}
			})
		}).Upload(uplInput)
		return errors.Wrap(err, "upload to S3")
	case S3ProviderGCS:
		// using minio client with GCS because it
		// allows to disable chuncks muiltipertition for upload
		mc, err := minio.NewWithRegion(GCSEndpointURL, s.opts.Credentials.AccessKeyID, s.opts.Credentials.SecretAccessKey, true, s.opts.Region)
		if err != nil {
			return errors.Wrap(err, "NewWithRegion")
		}
		putOpts := minio.PutObjectOptions{
			StorageClass: s.opts.StorageClass,
		}

		// Enable server-side encryption if configured
		sse := s.opts.ServerSideEncryption
		if sse != nil {
			if sse.SseAlgorithm == s3.ServerSideEncryptionAwsKms {
				sseKms, err := encrypt.NewSSEKMS(sse.KmsKeyID, nil)
				if err != nil {
					return errors.Wrap(err, "Could not create SSE KMS")
				}
				putOpts.ServerSideEncryption = sseKms
			} else if sse.SseCustomerAlgorithm != "" {
				decodedKey, err := base64.StdEncoding.DecodeString(sse.SseCustomerKey)
				if err != nil {
					return errors.Wrap(err, "SseCustomerAlgorithm specified with invalid SseCustomerKey")
				}

				sseCus, err := encrypt.NewSSEC(decodedKey)
				if err != nil {
					return errors.Wrap(err, "Could not create SSE-C SSE key")
				}
				putOpts.ServerSideEncryption = sseCus
			}
		}

		_, err = mc.PutObject(s.opts.Bucket, path.Join(s.opts.Prefix, name), data, -1, putOpts)
		return errors.Wrap(err, "upload to GCS")
	}
}

func (s *S3) List(prefix, suffix string) ([]storage.FileInfo, error) {
	prfx := path.Join(s.opts.Prefix, prefix)

	if prfx != "" && !strings.HasSuffix(prfx, "/") {
		prfx = prfx + "/"
	}

	lparams := &s3.ListObjectsInput{
		Bucket: aws.String(s.opts.Bucket),
	}

	if prfx != "" {
		lparams.Prefix = aws.String(prfx)
	}

	var files []storage.FileInfo
	err := s.s3s.ListObjectsPages(lparams,
		func(page *s3.ListObjectsOutput, lastPage bool) bool {
			for _, o := range page.Contents {
				f := aws.StringValue(o.Key)
				f = strings.TrimPrefix(f, aws.StringValue(lparams.Prefix))
				if len(f) == 0 {
					continue
				}
				if f[0] == '/' {
					f = f[1:]
				}

				if strings.HasSuffix(f, suffix) {
					files = append(files, storage.FileInfo{
						Name: f,
						Size: aws.Int64Value(o.Size),
					})
				}
			}
			return true
		})
	if err != nil {
		return nil, err
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
		if sse.SseAlgorithm == s3.ServerSideEncryptionAwsKms {
			copyOpts.ServerSideEncryption = aws.String(sse.SseAlgorithm)
			copyOpts.SSEKMSKeyId = aws.String(sse.KmsKeyID)
		} else if sse.SseCustomerAlgorithm != "" {
			copyOpts.SSECustomerAlgorithm = aws.String(sse.SseCustomerAlgorithm)
			decodedKey, err := base64.StdEncoding.DecodeString(sse.SseCustomerKey)
			copyOpts.SSECustomerKey = aws.String(string(decodedKey[:]))
			if err != nil {
				return errors.Wrap(err, "SseCustomerAlgorithm specified with invalid SseCustomerKey")
			}
			keyMD5 := md5.Sum(decodedKey[:])
			copyOpts.SSECustomerKeyMD5 = aws.String(base64.StdEncoding.EncodeToString(keyMD5[:]))

			copyOpts.CopySourceSSECustomerAlgorithm = copyOpts.SSECustomerAlgorithm
			copyOpts.CopySourceSSECustomerKey = copyOpts.SSECustomerKey
			copyOpts.CopySourceSSECustomerKeyMD5 = copyOpts.SSECustomerKeyMD5
		}
	}

	_, err := s.s3s.CopyObject(copyOpts)

	return err
}

func (s *S3) FileStat(name string) (inf storage.FileInfo, err error) {
	headOpts := &s3.HeadObjectInput{
		Bucket: aws.String(s.opts.Bucket),
		Key:    aws.String(path.Join(s.opts.Prefix, name)),
	}

	sse := s.opts.ServerSideEncryption
	if sse != nil && sse.SseCustomerAlgorithm != "" {
		headOpts.SSECustomerAlgorithm = aws.String(sse.SseCustomerAlgorithm)
		decodedKey, err := base64.StdEncoding.DecodeString(sse.SseCustomerKey)
		headOpts.SSECustomerKey = aws.String(string(decodedKey[:]))
		if err != nil {
			return inf, errors.Wrap(err, "SseCustomerAlgorithm specified with invalid SseCustomerKey")
		}
		keyMD5 := md5.Sum(decodedKey[:])
		headOpts.SSECustomerKeyMD5 = aws.String(base64.StdEncoding.EncodeToString(keyMD5[:]))
	}

	h, err := s.s3s.HeadObject(headOpts)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			return inf, storage.ErrNotExist
		}

		return inf, errors.Wrap(err, "get S3 object header")
	}
	inf.Name = name
	inf.Size = aws.Int64Value(h.ContentLength)

	if inf.Size == 0 {
		return inf, storage.ErrEmpty
	}
	if aws.BoolValue(h.DeleteMarker) {
		return inf, errors.New("file has delete marker")
	}

	return inf, nil
}

type errGetObj error

type partReader struct {
	fname   string
	getSess func() (*s3.S3, error)
	l       *log.Event
	opts    *Conf
	fsize   int64
	written int64
	buf     []byte
	pause   int32

	taskq   chan chunkMeta
	resultq chan chunk
	errc    chan error
	close   chan struct{}
	unpause chan struct{}
}

func (s *S3) newPartReader(fname string, fsize int64, chunkSize int) *partReader {
	return &partReader{
		l:     s.log,
		buf:   make([]byte, chunkSize),
		opts:  &s.opts,
		fname: fname,
		fsize: fsize,
		getSess: func() (*s3.S3, error) {
			sess, err := s.s3session()
			if err != nil {
				return nil, err
			}
			sess.Client.Config.HTTPClient.Timeout = time.Second * 60
			return sess, nil
		},
	}
}

type chunkMeta struct {
	start   int64
	end     int64
	attempt int
}

type chunk struct {
	r    io.ReadCloser
	meta chunkMeta
}

type chunksBuf []*chunk

func (b chunksBuf) Len() int           { return len(b) }
func (b chunksBuf) Less(i, j int) bool { return b[i].meta.start < b[j].meta.start }
func (b chunksBuf) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b *chunksBuf) Push(x any)        { *b = append(*b, x.(*chunk)) }
func (b *chunksBuf) Pop() any {
	old := *b
	n := len(old)
	x := old[n-1]
	*b = old[0 : n-1]
	return x
}

func (s *S3) SourceReader(name string) (io.ReadCloser, error) {
	fstat, err := s.FileStat(name)
	if err != nil {
		return nil, errors.Wrap(err, "get file stat")
	}

	r, w := io.Pipe()

	go func() {
		pr := s.newPartReader(name, fstat.Size, downloadChuckSize)

		cc := runtime.GOMAXPROCS(0)
		if s.opts.NumDownloadWorkers > 0 {
			cc = s.opts.NumDownloadWorkers
		}

		cc = 1
		pr.Run(cc)

		exitErr := io.EOF
		defer func() {
			w.CloseWithError(exitErr)
			pr.Reset()
		}()

		cbuf := &chunksBuf{}
		heap.Init(cbuf)

		buffThrottle := cc * 20

		for {
			select {
			case rs := <-pr.resultq:
				// Although chunks are requested concurrently they must be written sequentially
				// to the destination as it is not necessary a file (decompress, mongorestore etc.).
				// If it is not its turn (previous chunks weren't written yet) the chunk will be
				// added to the buffer to wait. If the buffer grows too much the scheduling of new
				// chunks will be paused for buffer to be handled.
				if rs.meta.start != pr.written {
					s.log.Debug("=> add to buf (%d)", len(*cbuf))
					heap.Push(cbuf, &rs)
					if len(*cbuf) == buffThrottle && pr.PauseSch() {
						s.log.Debug("buffer is full (%d), pause the new chunks scheduling until it's handled", len(*cbuf))
					}
					continue
				}

				err := pr.writeChunk(&rs, w, downloadRetries)
				if err != nil {
					exitErr = errors.Wrapf(err, "SourceReader: copy bytes %d-%d from resoponse", rs.meta.start, rs.meta.end)
					return
				}

				// check if we can send something from the buffer
				for len(*cbuf) > 0 && []*chunk(*cbuf)[0].meta.start == pr.written {
					r := heap.Pop(cbuf).(*chunk)
					err := pr.writeChunk(r, w, downloadRetries)
					if err != nil {
						exitErr = errors.Wrapf(err, "SourceReader: copy bytes %d-%d from resoponse buffer", r.meta.start, r.meta.end)
						return
					}
				}

				if len(*cbuf) == 0 && pr.UnpauseSch() {
					s.log.Debug("scheduling unpaused")
				}

				// we've read all bytes in the object
				if pr.written >= pr.fsize {
					return
				}

			case err := <-pr.errc:
				exitErr = errors.Wrapf(err, "SourceReader: download '%s/%s'", s.opts.Bucket, name)
				return
			}
		}
	}()

	return r, nil
}

func (pr *partReader) PauseSch() bool {
	return atomic.CompareAndSwapInt32(&pr.pause, 0, 1)
}

func (pr *partReader) UnpauseSch() bool {
	if atomic.CompareAndSwapInt32(&pr.pause, 2, 0) {
		pr.unpause <- struct{}{}
		return true
	}
	return atomic.CompareAndSwapInt32(&pr.pause, 1, 0)
}

func (pr *partReader) Run(concurrency int) {
	pr.taskq = make(chan chunkMeta, concurrency)
	pr.resultq = make(chan chunk)
	pr.errc = make(chan error)
	pr.close = make(chan struct{})

	go func() {
		for sent := int64(0); sent <= pr.fsize; {
			select {
			case <-pr.close:
				return
			default:
			}
			pr.taskq <- chunkMeta{sent, sent + downloadChuckSize - 1, 0}
			sent += downloadChuckSize
		}
	}()

	for i := 0; i < concurrency; i++ {
		go pr.worker()
	}
}

func (pr *partReader) Reset() {
	close(pr.close)
}

func (pr *partReader) writeChunk(r *chunk, to io.Writer, retry int) error {
	if r == nil || r.r == nil {
		return nil
	}

	b, err := io.CopyBuffer(to, r.r, pr.buf)
	pr.written += b
	r.r.Close()

	return err
}

func (pr *partReader) worker() {
	sess, err := pr.getSess()
	if err != nil {
		pr.errc <- errors.Wrap(err, "create session")
		return
	}
	for {
		select {
		case ch := <-pr.taskq:
			if ch.attempt > 0 {
				time.Sleep(time.Second * time.Duration(ch.attempt))
				pr.l.Debug("recreate session")
				sess, err = pr.getSess()
				if err != nil {
					pr.errc <- errors.Wrap(err, "create session")
					return
				}
			}
			r, err := pr.retryChunk(sess, ch.start, ch.end, downloadRetries)
			if err != nil {
				pr.errc <- err
				return
			}

			pr.resultq <- chunk{r: r, meta: ch}

		case <-pr.close:
			return
		}
	}
}

func (pr *partReader) retryChunk(s *s3.S3, start, end int64, retries int) (r io.ReadCloser, err error) {
	for i := 0; i < retries; i++ {
		r, err = pr.tryChunk(s, start, end)
		if err == nil {
			return r, nil
		}

		pr.l.Warning("retryChunk got %v, try to reconnect in %v", err, time.Second*time.Duration(i))
		time.Sleep(time.Second * time.Duration(i))
		s, err = pr.getSess()
		if err != nil {
			pr.l.Warning("recreate session err: %v", err)
			continue
		}
		pr.l.Info("session recreated, resuming download")
	}

	return nil, err
}

func (pr *partReader) tryChunk(s *s3.S3, start, end int64) (r io.ReadCloser, err error) {
	// just quickly retry w/o new session in case of fail.
	// more sophisticated retry on a caller side.
	const retry = 2
	for i := 0; i < retry; i++ {
		r, err = pr.getChunk(s, start, end)

		if err == nil || err == io.EOF {
			return r, nil
		}

		switch err.(type) {
		case errGetObj:
			return r, err
		}

		pr.l.Warning("failed to download chunk %d-%d", start, end)
	}

	return nil, errors.Wrapf(err, "failed to download chunk %d-%d (of %d) after %d retries", start, end, pr.fsize, retry)
}

func (pr *partReader) getChunk(s *s3.S3, start, end int64) (io.ReadCloser, error) {
	getObjOpts := &s3.GetObjectInput{
		Bucket: aws.String(pr.opts.Bucket),
		Key:    aws.String(path.Join(pr.opts.Prefix, pr.fname)),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	}

	sse := pr.opts.ServerSideEncryption
	if sse != nil && sse.SseCustomerAlgorithm != "" {
		getObjOpts.SSECustomerAlgorithm = aws.String(sse.SseCustomerAlgorithm)
		decodedKey, err := base64.StdEncoding.DecodeString(sse.SseCustomerKey)
		getObjOpts.SSECustomerKey = aws.String(string(decodedKey[:]))
		if err != nil {
			return nil, errors.Wrap(err, "SseCustomerAlgorithm specified with invalid SseCustomerKey")
		}
		keyMD5 := md5.Sum(decodedKey[:])
		getObjOpts.SSECustomerKeyMD5 = aws.String(base64.StdEncoding.EncodeToString(keyMD5[:]))
	}

	s3obj, err := s.GetObject(getObjOpts)
	if err != nil {
		// if object size is undefined, we would read
		// until HTTP code 416 (Requested Range Not Satisfiable)
		var er awserr.RequestFailure
		if errors.As(err, &er) && er.StatusCode() == http.StatusRequestedRangeNotSatisfiable {
			return nil, io.EOF
		}
		pr.l.Warning("errGetObj Err: %v", err)
		return nil, errGetObj(err)
	}
	defer s3obj.Body.Close()

	if sse != nil {
		if sse.SseAlgorithm == s3.ServerSideEncryptionAwsKms {
			s3obj.ServerSideEncryption = aws.String(sse.SseAlgorithm)
			s3obj.SSEKMSKeyId = aws.String(sse.KmsKeyID)
		} else if sse.SseCustomerAlgorithm != "" {
			s3obj.SSECustomerAlgorithm = aws.String(sse.SseCustomerAlgorithm)
			decodedKey, _ := base64.StdEncoding.DecodeString(sse.SseCustomerKey)
			// We don't pass in the key in this case, just the MD5 hash of the key
			// for verification
			// s3obj.SSECustomerKey = aws.String(string(decodedKey[:]))
			keyMD5 := md5.Sum(decodedKey[:])
			s3obj.SSECustomerKeyMD5 = aws.String(base64.StdEncoding.EncodeToString(keyMD5[:]))
		}
	}

	buf := newBuffer()
	_, err = io.Copy(buf, s3obj.Body)
	if err != nil {
		return nil, errors.Wrap(err, "copy")
	}
	return buf, nil
}

type buffer struct {
	*bytes.Buffer
}

func newBuffer() *buffer {
	return &buffer{bufferPool.Get().(*bytes.Buffer)}
}

func (b *buffer) Close() error {
	b.Buffer.Reset()
	bufferPool.Put(b.Buffer)
	return nil
}

var bufferPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// Delete deletes given file.
// It returns storage.ErrNotExist if a file isn't exists
func (s *S3) Delete(name string) error {
	_, err := s.s3s.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.opts.Bucket),
		Key:    aws.String(path.Join(s.opts.Prefix, name)),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				return storage.ErrNotExist
			}
		}
		return errors.Wrapf(err, "delete '%s/%s' file from S3", s.opts.Bucket, name)
	}

	return nil
}

func (s *S3) s3session() (*s3.S3, error) {
	sess, err := s.session()
	if err != nil {
		return nil, errors.Wrap(err, "create aws session")
	}

	return s3.New(sess), nil
}

func (s *S3) session() (*session.Session, error) {
	var providers []credentials.Provider

	// if we have credentials, set them first in the providers list
	if s.opts.Credentials.AccessKeyID != "" && s.opts.Credentials.SecretAccessKey != "" {
		providers = append(providers, &credentials.StaticProvider{Value: credentials.Value{
			AccessKeyID:     s.opts.Credentials.AccessKeyID,
			SecretAccessKey: s.opts.Credentials.SecretAccessKey,
			SessionToken:    "",
		}})
	}

	ec2Session, err := session.NewSession()
	if err != nil {
		return nil, errors.WithMessage(err, "new session")
	}

	// allow fetching credentials from env variables and ec2 metadata endpoint
	providers = append(providers, &credentials.EnvProvider{})
	providers = append(providers, &ec2rolecreds.EC2RoleProvider{
		Client: ec2metadata.New(ec2Session),
	})

	httpClient := &http.Client{}
	if s.opts.InsecureSkipTLSVerify {
		httpClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}
	}

	return session.NewSession(&aws.Config{
		Region:           aws.String(s.opts.Region),
		Endpoint:         aws.String(s.opts.EndpointURL),
		Credentials:      credentials.NewChainCredentials(providers),
		S3ForcePathStyle: aws.Bool(true),
		HTTPClient:       httpClient,
		LogLevel:         aws.LogLevel(SDKLogLevel(s.opts.DebugLogLevels, nil)),
		Logger:           awsLogger(s.log),
	})
}

func awsLogger(l *log.Event) aws.Logger {
	if l == nil {
		return aws.NewDefaultLogger()
	}

	return aws.LoggerFunc(func(xs ...interface{}) {
		if len(xs) == 0 {
			return
		}

		msg := "%v"
		for i := len(xs) - 1; i != 0; i++ {
			msg += " %v"
		}

		l.Debug(msg, xs...)
	})
}
