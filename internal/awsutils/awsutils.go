package awsutils

import (
	"fmt"
	"io"
	"log"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
)

var (
	FileNotFoundError = fmt.Errorf("File not found")
	awsSession        *session.Session
	lock              = &sync.Mutex{}
)

func GetAWSSession() (*session.Session, error) {
	// Initialize a session in us-west-2 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	var err error
	lock.Lock()
	defer lock.Unlock()
	if awsSession == nil {
		awsSession, err = session.NewSession(&aws.Config{})
	}
	if err != nil {
		return nil, err
	}
	return awsSession, nil
}

func BucketExists(svc *s3.S3, bucketname string) (bool, error) {
	input := &s3.ListBucketsInput{}

	result, err := svc.ListBuckets(input)
	if err != nil {
		return false, err
	}
	for _, bucket := range result.Buckets {
		if *bucket.Name == bucketname {
			return true, nil
		}
	}
	return false, nil
}

func CreateBucket(svc *s3.S3, bucket string) error {
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return errors.Wrapf(err, "Unable to create bucket %q", bucket)
	}

	err = svc.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return errors.Wrap(err, ("error while waiting the S3 bucket to be created"))
	}
	return nil
}

func S3Stat(svc *s3.S3, bucket, filename string) (*s3.Object, error) {
	resp, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(bucket)})
	if err != nil {
		return nil, err
	}

	for _, item := range resp.Contents {
		if *item.Key == filename {
			return item, nil
		}
	}
	return nil, FileNotFoundError
}

func DeleteFile(svc *s3.S3, bucket, filename string) error {
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(filename)})
	if err != nil {
		return errors.Wrapf(err, "unable to delete object %q from bucket %q", filename, bucket)
	}

	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
	})
	if err != nil {
		return errors.Wrapf(err, "file %s was not deleted from the %s bucket", filename, bucket)
	}
	return nil
}

func DeleteBucket(svc *s3.S3, bucket string) error {
	_, err := svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return errors.Wrapf(err, "unable to delete bucket %q", bucket)
	}

	err = svc.WaitUntilBucketNotExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return errors.Wrapf(err, "error occurred while waiting for bucket to be deleted, %s", bucket)
	}
	return nil
}

func DownloadFile(svc *s3.S3, bucket, file string, writer io.WriterAt) (int64, error) {
	downloader := s3manager.NewDownloaderWithClient(svc)

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(file),
	}

	return downloader.Download(writer, input)
}

func EmptyBucket(svc *s3.S3, bucket string) error {
	params := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	}
	for {
		objects, err := svc.ListObjects(params)
		if err != nil {
			return err
		}
		if len((*objects).Contents) == 0 {
			return nil
		}

		objectsToDelete := make([]*s3.ObjectIdentifier, 0, 1000)
		for _, object := range (*objects).Contents {
			obj := s3.ObjectIdentifier{
				Key: object.Key,
			}
			objectsToDelete = append(objectsToDelete, &obj)
		}
		deleteArray := s3.Delete{Objects: objectsToDelete}
		deleteParams := &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &deleteArray,
		}
		_, err = svc.DeleteObjects(deleteParams)
		if err != nil {
			return err
		}
		if *(*objects).IsTruncated {
			params.Marker = (*deleteParams).Delete.Objects[len((*deleteParams).Delete.Objects)-1].Key
		} else {
			break
		}
	}
	return nil
}

func Diag(params ...interface{}) {
	if testing.Verbose() {
		log.Printf(params[0].(string), params[1:]...)
	}
}
