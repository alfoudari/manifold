package stream

import (
	"os"
	// "fmt"
	"time"
	"strings"
	"bytes"
	"path/filepath"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	log "github.com/sirupsen/logrus"
)

type S3 struct {
	Region     string
	BucketName string
	Config     *S3Config
	Args       map[string]string
	Sess       *session.Session
	buffer 	   *buffer
}

type S3Config struct {
	Folder string
	CommitFileSize int
	CommitDuration int
	UploadEvery int
}

type buffer struct {
	path string
	messages chan string
}

func (s *S3) Connect() (err error) {
	s.buffer = &buffer{}
	// overwrite buffer.path with Args, if specified
	if val, ok := s.Args["bufferPath"]; ok {
		s.buffer.path = val
	} else {
		// default
		s.buffer.path = "/tmp/manifold/aws_s3/"
	}

	// create messages channel
	s.buffer.messages = make(chan string, 1000)
	// create a collector
	go s.collector()
	// create an uploader
	go s.uploader()

	return
}

func (s *S3) Disconnect() (err error) {
	close(s.buffer.messages)
	return
}

func (s *S3) Write(message string) (err error) {
	s.buffer.messages <- message
	return
}

func (s *S3) Info() {
	log.Info("S3.BucketName: ", s.BucketName)
	log.Infof("S3Config.CommitFileSize: every %d KB\n", s.Config.CommitFileSize)
	log.Infof("S3Config.CommitDuration: every %d minutes\n", s.Config.CommitDuration)
	log.Infof("S3Config.UploadEvery: %d seconds\n", s.Config.UploadEvery)
}

// Receive data on messages channel and write them
// to buf.path. 
//
// Files are aggregated on a 5 minutes interval.
func (s *S3) collector() {
	// create buf.path if it doesn't exist
	err := os.MkdirAll(s.buffer.path, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}

	timeCommitted := time.Now()
	for {
		// read buf.messages channel
		msg, ok := <-s.buffer.messages
		if ok == false {
			return // channel closed
		}

		// check if file 'buffer' exists
		path := filepath.Join(s.buffer.path, "buffer")
		exists, err := fileExists(path)
		if err != nil {
			log.Fatal(err)
		}

		// commit buffer if it's >= Config.CommitFileSize KB
		// or time elapsed >= Config.CommitDuration minutes
		info, err := os.Stat(path)
		fileSizeReached := info.Size() >= int64(s.Config.CommitFileSize) * 1024
		durationElapsed := int(time.Since(timeCommitted).Minutes()) >= s.Config.CommitDuration
		if exists && (fileSizeReached || durationElapsed) {
			// current point in time
			currentTime := time.Now()
			// organize buffer by creating a folder for each day
			commitDir := filepath.Join(s.buffer.path, currentTime.Format("2006-01-02"))
			// create the day directory if it doesn't exists
			err := os.MkdirAll(commitDir, os.ModePerm)
			if err != nil {
				log.Fatal(err)
			}

			// rename buffer to the current time in nanoseconds
			commitPath := filepath.Join(commitDir, currentTime.Format("150405.000000000"))
			err = os.Rename(path, commitPath)
			if err != nil {
				log.Fatal(err)
			}

			timeCommitted = time.Now()
			log.Info("Committed file ", commitPath)
		}
 
		// append (or create) to 'buffer'
		err = appendFile(path, msg+"\n")
		if err != nil {
			log.Fatal(err)
		}
	}
}

// Scan buf.path for files and upload them once found.
func (s *S3) uploader() {
	uploader := s3manager.NewUploader(s.Sess)
	for {
		var files []string
		err := filepath.Walk(s.buffer.path, func(path string, info os.FileInfo, err error) error {
			if info.IsDir() || info.Name() == "buffer" {
				return nil
			}

			files = append(files, path)

			return nil
		})
		if err != nil {
			panic(err)
		}
		for _, file := range files {
			// truncate buf.path (S3 path)
			key := strings.Replace(file, s.buffer.path, "", 1)
			// read file
			body, err := ioutil.ReadFile(file)
			if err != nil {
				log.Fatalln("Couldn't read file: ", file)
			}
			// upload the file to S3
			_, err = uploader.Upload(&s3manager.UploadInput{
				Bucket: aws.String(s.BucketName),
				Key:    aws.String(key),
				Body:   bytes.NewReader(body),
			})
			if err != nil {
				log.Fatalln("Failed to upload file: ", file)
			}
			// file uploaded successfully
			err = os.Remove(file)
			if err != nil {
				log.Errorln("Couldn't remove file: ", file)
			}

			log.Info("Uploaded ", key)
		}
		time.Sleep(time.Duration(s.Config.UploadEvery)*time.Second)
	}
}

// If file doesn't exist, create it, or append to it
func appendFile(path string, text string) (err error) {
    f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
		return
    }
    if _, err := f.Write([]byte(text)); err != nil {
		return err
    }
    if err := f.Close(); err != nil {
		return err
	}
	return
}

func fileExists(path string) (exists bool, err error) {
	info, err := os.Stat(path)
	exists = !os.IsNotExist(err) && !info.IsDir()
	if !exists {
		err = nil
	}

	return
}