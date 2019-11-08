package stream


import (
	"strings"
	"net/http"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	log "github.com/sirupsen/logrus"
)

type S3 struct {
	URL    string
	Header http.Header
	Args   map[string]string
	sess   *session.Session
}

func (s *S3) Connect() (err error) {
	// AWS setup
	s.sess, err = session.NewSession(&aws.Config{
		Region:      aws.String(s.Args["awsRegion"]),
		Credentials: credentials.NewStaticCredentials(s.Args["awsAccessKey"], s.Args["awsSecretKey"], ""),
	})
	if err != nil {
		// Handle Session creation error
		log.Errorln("Error creating session: ", err)
	}
	return
}

func (s *S3) Disconnect() (err error) {
	return nil
}

func (s *S3) Write(message string) (err error) {
	uploader := s3manager.NewUploader(s.sess)

	// Upload the file to S3.
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.Args["bucketName"]),
		Key:    aws.String(s.Args["key"]),
		Body:   strings.NewReader(message),
	})
	if err != nil {
		log.Errorln("Failed to upload file: ", err)
	}

	return
}
