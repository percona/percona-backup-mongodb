package s3writer

import (
	"bytes"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
)

const (
	maxRetries = 3
)

var (
	s3MaxChunkSize = 5 * 1024 * 1024 // 5 Mb
)

type S3Writer struct {
	s3Session               *session.Session
	svc                     *s3.S3
	bucket                  string
	filename                string
	reader                  io.Reader
	s3MultipartUploadOutput *s3.CreateMultipartUploadOutput
	completedResponse       *s3.CompleteMultipartUploadOutput
	completedParts          []*s3.CompletedPart
	bytesSent               int
	currentPartNumber       int
	buffer                  []byte
}

func Open(sess *session.Session, bucket, filename string) (*S3Writer, error) {

	svc := s3.New(sess)

	return &S3Writer{
		s3Session: sess,
		svc:       svc,
		bucket:    bucket,
		filename:  filename,
		buffer:    make([]byte, 0, s3MaxChunkSize),
	}, nil
}

func (w *S3Writer) Close() error {
	var err error
	if len(w.buffer) > 0 {
		// If this is the part number 0 and we are closing the buffer, it means than the buffer size
		// is smaller than the minimum required for a multipart upload (5Mb). In that case, upload
		// the file using s3manager.UploadInput
		if w.currentPartNumber == 0 {
			return w.uploadSinglePart()
		}
		// currentPartNumber is > 0, upload the last chunk and then call completeMultipartUpload
		w.currentPartNumber++
		completedPart, err := uploadPart(w.svc, w.s3MultipartUploadOutput, w.buffer, w.currentPartNumber)
		if err != nil {
			err := abortMultipartUpload(w.svc, w.s3MultipartUploadOutput)
			if err != nil {
				return err
			}
			return err
		}
		w.completedParts = append(w.completedParts, completedPart)
	}
	w.completedResponse, err = completeMultipartUpload(w.svc, w.s3MultipartUploadOutput, w.completedParts)
	if err != nil {
		return err
	}
	return nil
}

func (w *S3Writer) Write(inBuffer []byte) (int, error) {
	var err error
	inBufferLen := len(inBuffer)
	remainingBufferSize := s3MaxChunkSize - len(w.buffer)

	if len(w.buffer)+inBufferLen < s3MaxChunkSize {
		w.buffer = append(w.buffer, inBuffer...)
		return inBufferLen, nil
	}

	writeBuffer := make([]byte, len(w.buffer), s3MaxChunkSize)
	copy(writeBuffer, w.buffer)
	writeBuffer = append(writeBuffer, inBuffer[:remainingBufferSize]...)

	remainingInBufferSize := len(inBuffer) - remainingBufferSize
	w.buffer = make([]byte, remainingInBufferSize, s3MaxChunkSize)
	copy(w.buffer, inBuffer[remainingBufferSize:])

	// The S3 multipart upload is not being created on Open() because we cannot use it
	// for chunks smaller than 5 Mb (S3 multipart limitation)
	// For this reason, we only create the multipart uploader only when we know there
	// are at least 5Mb in the buffer.
	if w.s3MultipartUploadOutput == nil {
		input := &s3.CreateMultipartUploadInput{
			Bucket:      aws.String(w.bucket),
			Key:         aws.String(w.filename),
			ContentType: aws.String("application/octet-stream"),
		}
		w.s3MultipartUploadOutput, err = w.svc.CreateMultipartUpload(input)
		if err != nil {
			return 0, errors.Wrap(err, "Cannot create S3 multipart upload")
		}
	}

	w.currentPartNumber++
	completedPart, err := uploadPart(w.svc, w.s3MultipartUploadOutput, writeBuffer, w.currentPartNumber)
	if err != nil {
		err := abortMultipartUpload(w.svc, w.s3MultipartUploadOutput)
		if err != nil {
			return 0, err
		}
		return 0, nil
	}
	w.bytesSent += len(writeBuffer)
	w.completedParts = append(w.completedParts, completedPart)

	return inBufferLen, nil
}

func (w *S3Writer) uploadSinglePart() error {
	uploader := s3manager.NewUploader(w.s3Session)
	reader := bytes.NewReader(w.buffer)
	upParams := &s3manager.UploadInput{
		Bucket: &w.bucket,
		Key:    &w.filename,
		Body:   reader,
	}

	_, err := uploader.Upload(upParams)
	return err
}
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
			tryNum++
		} else {
			return &s3.CompletedPart{
				ETag:       uploadResult.ETag,
				PartNumber: aws.Int64(int64(partNumber)),
			}, nil
		}
	}
	return nil, nil
}

func abortMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput) error {
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	_, err := svc.AbortMultipartUpload(abortInput)
	return err
}
