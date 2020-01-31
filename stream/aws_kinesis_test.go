package stream

import (
	// "fmt"
	"testing"
	// "net/http"
	// "net/http/httptest"

	// log "github.com/sirupsen/logrus"
	// "github.com/aws/aws-sdk-go/aws"
	// "github.com/aws/aws-sdk-go/aws/session"
	// "github.com/aws/aws-sdk-go/aws/credentials"
	// "github.com/aws/aws-sdk-go/aws/client"
	// "github.com/aws/aws-sdk-go/aws/client/metadata"
	// "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

type mockKinesisClient struct {
	kinesisiface.KinesisAPI
}

func TestKinesis_Connect(t *testing.T) {
	newSession = func(k *Kinesis) (sess interface{}) {
		sess = &mockKinesisClient{}
		return sess
	}

	src := Kinesis{
		ConsumerName: "operations",
		StreamARN:    "arn:aws:kinesis:us-east-1:999999999999:stream/suspicious_activities",
		AWSSess:      nil,
	}

	src.Connect()
}