package main

import (
	"os"
	"os/signal"

	"github.com/abstractpaper/manifold/stream"
	"github.com/abstractpaper/swissarmy"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	log "github.com/sirupsen/logrus"
)

func main() {
	// interrupt channel for OS signals
	interrupt := make(chan os.Signal, 1)
	// register interrupt channel to receive SIGINT and SIGKILL
	signal.Notify(interrupt, os.Interrupt, os.Kill)
	
	// aws config
	awsRegion := swissarmy.GetEnv("MANIFOLD_AWS_REGION", "us-east-1")
	awsAccessKey := swissarmy.GetEnv("MANIFOLD_AWS_ACCESS_KEY", "")
	awsSecretKey := swissarmy.GetEnv("MANIFOLD_AWS_SECRET_KEY", "")

	// AWS setup
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewStaticCredentials(awsAccessKey, awsSecretKey, ""),
	})
	if err != nil {
		log.Fatalln("Error creating session: ", err)
	}

	src := stream.Stdio{}

	dest := stream.Kinesis{
		AWSSess:    sess,
		Args:		map[string]string{
			"partitionKey": 	"partition1",
			"streamName":		"test",
		},
	}

	stream.Flow(&src, nil, &dest)

	// wait for interrupt signals
	<-interrupt
	log.Info("Interrupt received.")
}
