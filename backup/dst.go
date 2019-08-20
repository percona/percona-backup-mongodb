package backup

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"path"

	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4"

	"github.com/percona/percona-backup-mongodb/pbm"
)

// Destination returns io.WriterCloser for the given storage
func Destination(stg pbm.Storage, name string, compression pbm.CompressionType) (io.WriteCloser, error) {
	var wr io.WriteCloser

	switch stg.Type {
	case pbm.StorageFilesystem:
		filepath := path.Join(stg.Filesystem.Path, name)
		fw, err := os.Create(filepath)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot create destination file: %s", filepath)
		}
		wr = fw
	case pbm.StorageS3:
		// awsSession, err := awsutils.GetAWSSessionFromStorage(stg.S3)
		awsSession, err := session.NewSession(&aws.Config{
			Region:   aws.String(stg.S3.Region),
			Endpoint: aws.String(stg.S3.EndpointURL),
			Credentials: credentials.NewStaticCredentials(
				stg.S3.Credentials.AccessKeyID,
				stg.S3.Credentials.SecretAccessKey,
				"",
			),
			S3ForcePathStyle: aws.Bool(true),
		})
		if err != nil {
			return nil, errors.Wrap(err, "cannot create AWS session")
		}
		// s3.Uploader runs synchronously and receives an io.Reader but here, we are implementing
		// writers so, we need to create an io.Pipe and run uploader.Upload in a go-routine
		pr, pw := io.Pipe()
		go func() {
			fmt.Println("Upload")
			_, err := s3manager.NewUploader(awsSession, func(u *s3manager.Uploader) {
				u.PartSize = 32 * 1024 * 1024 // 10MB part size
				u.LeavePartsOnError = true    // Don't delete the parts if the upload fails.
				u.Concurrency = 10
			}).Upload(&s3manager.UploadInput{
				Bucket: aws.String(stg.S3.Bucket),
				Key:    aws.String(name),
				Body:   pr,
			})
			// TODO: "return" error and log it on the upward levels
			if err != nil {
				log.Println("[ERROR] s3 upload:", err)
			}
		}()
		wr = pw
	}

	switch compression {
	case pbm.CompressionTypeGZIP:
		wr = gzip.NewWriter(wr)
	case pbm.CompressionTypeLZ4:
		wr = lz4.NewWriter(wr)
	case pbm.CompressionTypeSNAPPY:
		wr = snappy.NewWriter(wr)
	}

	// switch cypher {
	// case pbm.Cypher_CYPHER_NO_CYPHER:
	// 	//TODO: Add cyphers
	// }

	return wr, nil
}
