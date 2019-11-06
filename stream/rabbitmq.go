package stream

import (
	"net/http"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type RabbitMQ struct {
	URL     string
	Header  http.Header
	conn    *amqp.Connection
	channel *amqp.Channel
}

func (r *RabbitMQ) Connect() (err error) {
	// connect to rabbitmq
	log.Info("Establishing rabbitmq connection...")
	r.conn, err = amqp.Dial(r.URL)
	if err != nil {
		log.Fatal("RabbitMQ: Failed to connect: ", err)
	}
	r.channel, err = r.conn.Channel()
	if err != nil {
		log.Fatal("RabbitMQ: Failed to open a channel: ", err)
	}
	return
}

func (r *RabbitMQ) Disconnect() (err error) {
	if r.conn == nil {
		log.Fatal("RabbitMQ.Disconnect(): conn is nil")
	}

	log.Info("Closing rabbitmq connection...")
	err = r.conn.Close()
	if err != nil {
		log.Error("RabbitMQ close error: ", err)
		return
	}
	log.Info("RabbitMQ connection closed.")

	return
}

// Write (publish) to a RabbitMQ exchange.
//
// Key Arguments:
//  exchange - exchange to publish to
//  key - routing key
//  message - message to publish
func (r *RabbitMQ) Write(kv map[string]string) (err error) {
	err = r.channel.Publish(
		kv["exchange"], // exchange
		kv["key"],      // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(kv["message"]),
		})

	if err != nil {
		log.Error("RabbitMQ: Failed to publish to channel: ", err)
		return
	}

	log.Info(kv["message"])

	return
}
