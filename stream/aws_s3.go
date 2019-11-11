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
	Args       map[string]string
	Sess       *session.Session
}

type buffer struct {
	path string
	messages chan string
}

var buf buffer

func (s *S3) Connect() (err error) {
	// overwrite buf.path with Args, if specified
	if val, ok := s.Args["localBufferPath"]; ok {
		buf.path = val
	} else {
		// default
		buf.path = "/tmp/manifold/aws_s3/"
	}

	// create messages channel
	buf.messages = make(chan string, 1000)
	// create a collector
	go s.collector(buf)
	// create an uploader
	go s.uploader()

	return
}

func (s *S3) Disconnect() (err error) {
	close(buf.messages)
	return
}

func (s *S3) Write(message string) (err error) {
	buf.messages <- message
	return
}

func (s *S3) Info() {
	log.Info("BucketName: ", s.BucketName)
}

// Receive data on messages channel and write them
// to buf.path. 
//
// Files are aggregated on a 5 minutes interval.
func (s *S3) collector(buf buffer) {
	// create buf.path if it doesn't exist
	err := os.MkdirAll(buf.path, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}

	for {
		// read buf.messages channel
		msg, ok := <-buf.messages
		if ok == false {
			return // channel closed
		}

		// check if file 'buffer' exists
		path := filepath.Join(buf.path, "buffer")
		exists, err := fileExists(path)
		if err != nil {
			log.Fatal(err)
		}

		// commit buffer if it's >= 100 KB
		info, err := os.Stat(path)
		if exists && info.Size() >= 100*1024 {
			// current point in time
			currentTime := time.Now()
			// organize buffer by creating a folder for each day
			commitDir := filepath.Join(buf.path, currentTime.Format("2006-01-02"))
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
	var stat stat
	uploader := s3manager.NewUploader(s.Sess)
	for {
		var files []string
		err := filepath.Walk(buf.path, func(path string, info os.FileInfo, err error) error {
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
			key := strings.Replace(file, buf.path, "", 1)
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

			stat.count++
			if stat.count % 1 == 0 {
				log.Println("Upload count: ", stat.count)
			}
		}
		time.Sleep(time.Duration(5)*time.Second)
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