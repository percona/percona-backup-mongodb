package s3

import (
	"bytes"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

const (
	s3MaxChunkSize = int64(5 * 1024 * 1024) // 5 Mb
	maxRetries     = 3
)

type S3Writer struct {
	svc                     *s3.S3
	bucket                  string
	filename                string
	reader                  io.Reader
	s3MultipartUploadOutput *s3.CreateMultipartUploadOutput
	completedParts          []*s3.CompletedPart
	n                       int
	completedResponse       *s3.CompleteMultipartUploadOutput
	partNumber              int
}

func Open(svc *s3.S3, bucket, filename string) (*S3Writer, error) {
	fileType := "application/octet-stream"

	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(filename),
		ContentType: aws.String(fileType),
	}

	resp, err := svc.CreateMultipartUpload(input)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot create S3 multipart upload")
	}

	return &S3Writer{
		svc:                     svc,
		bucket:                  bucket,
		filename:                filename,
		s3MultipartUploadOutput: resp,
	}, nil
}

func (w *S3Writer) Close() error {
	var err error
	w.completedResponse, err = completeMultipartUpload(w.svc, w.s3MultipartUploadOutput, w.completedParts)
	if err != nil {
		return nil
	}
	return nil
}

func (w *S3Writer) Write(buf []byte) (int, error) {
	w.partNumber++
	completedPart, err := uploadPart(w.svc, w.s3MultipartUploadOutput, buf, w.partNumber)
	if err != nil {
		err := abortMultipartUpload(w.svc, w.s3MultipartUploadOutput)
		if err != nil {
			fmt.Println(err.Error())
		}
		return 0, nil
	}
	w.completedParts = append(w.completedParts, completedPart)

	return len(buf), nil
}

// func maini() {
// 	creds := credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, "")
// 	_, err := creds.Get()
// 	if err != nil {
// 		fmt.Printf("bad credentials: %s", err)
// 	}
// 	cfg := aws.NewConfig().WithRegion(awsBucketRegion).WithCredentials(creds)
// 	svc := s3.New(session.New(), cfg)
//
// 	file, err := os.Open("test.jpg")
// 	if err != nil {
// 		fmt.Printf("err opening file: %s", err)
// 		return
// 	}
// 	defer file.Close()
// 	fileInfo, _ := file.Stat()
// 	size := fileInfo.Size()
// 	buffer := make([]byte, size)
// 	fileType := http.DetectContentType(buffer)
// 	file.Read(buffer)
//
// 	path := "/media/" + file.Name()
// 	input := &s3.CreateMultipartUploadInput{
// 		Bucket:      aws.String(awsBucketName),
// 		Key:         aws.String(path),
// 		ContentType: aws.String(fileType),
// 	}
//
// 	resp, err := svc.CreateMultipartUpload(input)
// 	if err != nil {
// 		fmt.Println(err.Error())
// 		return
// 	}
// 	fmt.Println("Created multipart upload request")
//
// 	var curr, partLength int64
// 	var remaining = size
// 	var completedParts []*s3.CompletedPart
// 	partNumber := 1
// 	for curr = 0; remaining != 0; curr += partLength {
// 		if remaining < maxPartSize {
// 			partLength = remaining
// 		} else {
// 			partLength = maxPartSize
// 		}
// 		completedPart, err := uploadPart(svc, resp, buffer[curr:curr+partLength], partNumber)
// 		if err != nil {
// 			fmt.Println(err.Error())
// 			err := abortMultipartUpload(svc, resp)
// 			if err != nil {
// 				fmt.Println(err.Error())
// 			}
// 			return
// 		}
// 		remaining -= partLength
// 		partNumber++
// 		completedParts = append(completedParts, completedPart)
// 	}
//
// 	completeResponse, err := completeMultipartUpload(svc, resp, completedParts)
// 	if err != nil {
// 		fmt.Println(err.Error())
// 		return
// 	}
//
// 	fmt.Printf("Successfully uploaded file: %s\n", completeResponse.String())
// }

func completeMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return svc.CompleteMultipartUpload(completeInput)
}

func uploadPart(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNumber int) (*s3.CompletedPart, error) {
	tryNum := 1
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(fileBytes),
		Bucket:        resp.Bucket,
		Key:           resp.Key,
		PartNumber:    aws.Int64(int64(partNumber)),
		UploadId:      resp.UploadId,
		ContentLength: aws.Int64(int64(len(fileBytes))),
	}

	for tryNum <= maxRetries {
		uploadResult, err := svc.UploadPart(partInput)
		if err != nil {
			if tryNum == maxRetries {
				if aerr, ok := err.(awserr.Error); ok {
					return nil, aerr
				}
				return nil, err
			}
			fmt.Printf("Retrying to upload part #%v\n", partNumber)
			tryNum++
		} else {
			fmt.Printf("Uploaded part #%v\n", partNumber)
			return &s3.CompletedPart{
				ETag:       uploadResult.ETag,
				PartNumber: aws.Int64(int64(partNumber)),
			}, nil
		}
	}
	return nil, nil
}

func abortMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput) error {
	fmt.Println("Aborting multipart upload for UploadId#" + *resp.UploadId)
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	_, err := svc.AbortMultipartUpload(abortInput)
	return err
}
