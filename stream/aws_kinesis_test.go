package stream

import (
	// "fmt"
	"testing"
	// "net/http"
	// "net/http/httptest"

	// log "github.com/sirupsen/logrus"
	// "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	// "github.com/aws/aws-sdk-go/aws/credentials"
	// "github.com/aws/aws-sdk-go/aws/client"
	// "github.com/aws/aws-sdk-go/aws/client/metadata"
	// "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

type mockKinesisClient struct {
	kinesisiface.KinesisAPI
}

type mockKinesisSession struct {
	Kinesis
}

func (m *mockKinesisSession)

func TestKinesisRead(t *testing.T) {
	

	src.Connect()
}