package s3

import (
	"io"
	"io/ioutil"
	"net/url"
	"path"
	"runtime"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/minio/minio-go"
	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

const (
	// GCSEndpointURL is the endpoint url for Google Clound Strage service
	GCSEndpointURL = "storage.googleapis.com"

	defaultS3Region = "us-east-1"
)

type Conf struct {
	Provider             S3Provider  `bson:"provider,omitempty" json:"provider,omitempty" yaml:"provider,omitempty"`
	Region               string      `bson:"region" json:"region" yaml:"region"`
	EndpointURL          string      `bson:"endpointUrl,omitempty" json:"endpointUrl" yaml:"endpointUrl,omitempty"`
	Bucket               string      `bson:"bucket" json:"bucket" yaml:"bucket"`
	Prefix               string      `bson:"prefix,omitempty" json:"prefix,omitempty" yaml:"prefix,omitempty"`
	Credentials          Credentials `bson:"credentials" json:"credentials,omitempty" yaml:"credentials"`
	ServerSideEncryption *AWSsse     `bson:"serverSideEncryption" json:"serverSideEncryption,omitempty" yaml:"serverSideEncryption,omitempty"`
}

type AWSsse struct {
	SseAlgorithm   string `bson:"sseAlgorithm" json:"sseAlgorithm" yaml:"sseAlgorithm"`
	KmsMasterKeyID string `bson:"kmsMasterKeyID" json:"kmsMasterKeyID" yaml:"kmsMasterKeyID"`
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

	return nil
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
}

func New(opts Conf) (*S3, error) {
	err := opts.Cast()
	if err != nil {
		return nil, errors.Wrap(err, "cast options")
	}

	return &S3{
		opts: opts,
	}, nil
}

func (s *S3) Save(name string, data io.Reader) error {
	switch s.opts.Provider {
	default:
		awsSession, err := s.sesseion()
		if err != nil {
			return errors.Wrap(err, "create AWS session")
		}
		cc := runtime.NumCPU() / 2
		if cc == 0 {
			cc = 1
		}

		uplInput := &s3manager.UploadInput{
			Bucket: aws.String(s.opts.Bucket),
			Key:    aws.String(path.Join(s.opts.Prefix, name)),
			Body:   data,
		}
		if s.opts.ServerSideEncryption != nil {
			sse := s.opts.ServerSideEncryption

			uplInput.SSECustomerAlgorithm = aws.String(sse.SseAlgorithm)
			if sse.SseAlgorithm == s3.ServerSideEncryptionAwsKms {
				uplInput.SSEKMSKeyId = aws.String(sse.KmsMasterKeyID)
			}
		}

		_, err = s3manager.NewUploader(awsSession, func(u *s3manager.Uploader) {
			u.PartSize = 10 * 1024 * 1024 // 10MB part size
			u.LeavePartsOnError = true    // Don't delete the parts if the upload fails.
			u.Concurrency = cc
		}).Upload(uplInput)
		return errors.Wrap(err, "upload to S3")
	case S3ProviderGCS:
		// using minio client with GCS because it
		// allows to disable chuncks muiltipertition for upload
		mc, err := minio.NewWithRegion(GCSEndpointURL, s.opts.Credentials.AccessKeyID, s.opts.Credentials.SecretAccessKey, true, s.opts.Region)
		if err != nil {
			return errors.Wrap(err, "NewWithRegion")
		}
		_, err = mc.PutObject(s.opts.Bucket, path.Join(s.opts.Prefix, name), data, -1, minio.PutObjectOptions{})
		return errors.Wrap(err, "upload to GCS")
	}
}

func (s *S3) Files(suffix string) ([][]byte, error) {
	s3s, err := s.s3session()
	if err != nil {
		return nil, errors.Wrap(err, "AWS session")
	}

	lparams := &s3.ListObjectsInput{
		Bucket:    aws.String(s.opts.Bucket),
		Delimiter: aws.String("/"),
	}
	if s.opts.Prefix != "" {
		lparams.Prefix = aws.String(s.opts.Prefix)
		if s.opts.Prefix[len(s.opts.Prefix)-1] != '/' {
			*lparams.Prefix += "/"
		}
	}

	var bcps [][]byte
	var berr error
	err = s3s.ListObjectsPages(lparams,
		func(page *s3.ListObjectsOutput, lastPage bool) bool {
			for _, o := range page.Contents {
				name := aws.StringValue(o.Key)
				if strings.HasSuffix(name, suffix) {
					s3obj, err := s3s.GetObject(&s3.GetObjectInput{
						Bucket: aws.String(s.opts.Bucket),
						Key:    aws.String(name),
					})
					if err != nil {
						berr = errors.Wrapf(err, "get object '%s'", name)
						return false
					}

					b, err := ioutil.ReadAll(s3obj.Body)
					if err != nil {
						berr = errors.Wrapf(err, "read object '%s'", name)
						return false
					}
					bcps = append(bcps, b)
				}
			}
			return true
		})

	if err != nil {
		return nil, errors.Wrap(err, "get backup list")
	}

	if berr != nil {
		return nil, errors.Wrap(berr, "metadata")
	}

	return bcps, nil
}

func (s *S3) List(prefix string) ([]string, error) {
	s3s, err := s.s3session()
	if err != nil {
		return nil, errors.Wrap(err, "AWS session")
	}

	lparams := &s3.ListObjectsInput{
		Bucket: aws.String(s.opts.Bucket),
	}
	if s.opts.Prefix != "" {
		lparams.Prefix = aws.String(s.opts.Prefix)
		if s.opts.Prefix[len(s.opts.Prefix)-1] != '/' {
			*lparams.Prefix += "/"
		}
	}

	if aws.StringValue(lparams.Prefix) != "" || prefix != "" {
		lparams.Prefix = aws.String(path.Join(aws.StringValue(lparams.Prefix), prefix))
	}

	var files []string
	err = s3s.ListObjectsPages(lparams,
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
				files = append(files, f)
			}
			return true
		})

	if err != nil {
		return nil, errors.Wrap(err, "get backup list")
	}

	return files, nil
}

func (s *S3) CheckFile(name string) error {
	s3s, err := s.s3session()
	if err != nil {
		return errors.Wrap(err, "AWS session")
	}

	h, err := s3s.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.opts.Bucket),
		Key:    aws.String(path.Join(s.opts.Prefix, name)),
	})
	if err != nil {
		return errors.Wrap(err, "get S3 object header")
	}

	if aws.Int64Value(h.ContentLength) == 0 {
		return errors.New("file empty")
	}
	if aws.BoolValue(h.DeleteMarker) {
		return errors.New("file has delete marker")
	}

	return nil
}

func (s *S3) SourceReader(name string) (io.ReadCloser, error) {
	s3s, err := s.s3session()
	if err != nil {
		return nil, errors.Wrap(err, "AWS session")
	}

	s3obj, err := s3s.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.opts.Bucket),
		Key:    aws.String(path.Join(s.opts.Prefix, name)),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "read '%s/%s' file from S3", s.opts.Bucket, name)
	}
	return s3obj.Body, nil
}

// Delete deletes given file.
// It returns storage.ErrNotExist if a file isn't exists
func (s *S3) Delete(name string) error {
	s3s, err := s.s3session()
	if err != nil {
		return errors.Wrap(err, "AWS session")
	}
	_, err = s3s.DeleteObject(&s3.DeleteObjectInput{
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
	sess, err := s.sesseion()
	if err != nil {
		return nil, errors.Wrap(err, "create aws session")
	}

	s3s, err := s.s3(sess)
	if err != nil {
		return nil, errors.Wrap(err, "create s3 session")
	}

	return s3s, nil
}

func (s *S3) sesseion() (*session.Session, error) {
	return session.NewSession(&aws.Config{
		Region:   aws.String(s.opts.Region),
		Endpoint: aws.String(s.opts.EndpointURL),
		Credentials: credentials.NewStaticCredentials(
			s.opts.Credentials.AccessKeyID,
			s.opts.Credentials.SecretAccessKey,
			"",
		),
		S3ForcePathStyle: aws.Bool(true),
	})
}

func (s *S3) s3(sess *session.Session) (*s3.S3, error) {
	s3s := s3.New(sess)

	if s.opts.ServerSideEncryption != nil {
		sse := s.opts.ServerSideEncryption

		sseopts := &s3.ServerSideEncryptionByDefault{
			SSEAlgorithm: aws.String(sse.SseAlgorithm),
		}

		if sse.SseAlgorithm == s3.ServerSideEncryptionAwsKms {
			sseopts.KMSMasterKeyID = aws.String(sse.KmsMasterKeyID)
		}

		_, err := s3s.PutBucketEncryption(&s3.PutBucketEncryptionInput{
			Bucket: aws.String(s.opts.Bucket),
			ServerSideEncryptionConfiguration: &s3.ServerSideEncryptionConfiguration{
				Rules: []*s3.ServerSideEncryptionRule{{
					ApplyServerSideEncryptionByDefault: sseopts,
				}},
			},
		})
		return nil, errors.Wrap(err, "set ServerSideEncryption")
	}

	return s3s, nil
}
