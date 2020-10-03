package stream

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDB represents a mongodb connection.
type MongoDB struct {
	// DB Host.
	Host string

	// DB Port.
	Port string

	// Duration to wait for a connection to be established.
	// Ex: 10 * time.Second
	ConnectTimeout time.Duration

	// Duration to wait for a connection to be terminated.
	DisconnectTimeout time.Duration

	Args map[string]string

	client *mongo.Client
}

// Info shows mongodb general info.
func (m *MongoDB) Info() {
	log.Info("Host: ", m.Host)
	log.Info("Port: ", m.Port)
}

// Connect establishes a new connection to mongodb.
func (m *MongoDB) Connect() (err error) {
	// connect to mongodb
	log.Info("Establishing mongodb connection...")

	// client instance
	uri := fmt.Sprintf("mongodb://%s:%s", m.Host, m.Port)
	m.client, err = mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		log.Error("MongoDB.Connect(): Failed to establish a client: ", err)
		return
	}

	// connect to client
	ctx, cancel := context.WithTimeout(context.Background(), m.ConnectTimeout)
	defer cancel()
	if err = m.client.Connect(ctx); err != nil {
		log.Error("MongoDB.Connect(): Failed to connect: ", err)
		return
	}

	return
}

// Disconnect terminates an established connection to mongodb.
func (m *MongoDB) Disconnect() (err error) {
	if m.client == nil {
		log.Error("MongoDB.Disconnect(): conn is nil")
		return
	}

	log.Info("Closing mongodb connection...")

	ctx, cancel := context.WithTimeout(context.Background(), m.DisconnectTimeout)
	defer cancel()
	if err = m.client.Disconnect(ctx); err != nil {
		log.Error("MongoDB.Disconnect(): Failed to connect: ", err)
		return
	}

	return
}

// Write inserts records into mongodb.
//
// Key Arguments:
//  exchange - exchange to publish to
//  key - routing key
//  message - message to publish
func (m *MongoDB) Write(message string) (err error) {
	log.Info("Writing to mongodb: ", message)

	return
}
