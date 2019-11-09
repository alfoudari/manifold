package stream

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	log "github.com/sirupsen/logrus"
)

type S3 struct {
	Region     string
	BucketName string
	Args       map[string]string
	Sess       *session.Session
}

func (s *S3) Connect() (err error) {
	// local files -> S3 collector goroutine
	return
}

func (s *S3) Disconnect() (err error) {
	return
}

func (s *S3) Write(message string) (err error) {
	uploader := s3manager.NewUploader(s.Sess)

	// Upload the file to S3
	path := fmt.Sprintf("%s/test", s.Args["key"])
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(path),
		Body:   strings.NewReader(message),
	})
	if err != nil {
		log.Errorln("Failed to upload file: ", err)
	}

	return
}

func (s *S3) Info() {
	log.Info("BucketName: ", s.BucketName)
}
