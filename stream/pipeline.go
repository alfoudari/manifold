package stream

import (
	"os"
	"os/signal"
	"reflect"

	"github.com/abstractpaper/manifold/transform"
	swissFunc "github.com/abstractpaper/swissarmy/function"

	log "github.com/sirupsen/logrus"
)

// Source is an interface that must be implemented to
// flow data into the pipeline.
//
// Example sources: AWS Kinesis, RabbitMQ, WebSocket
type Source interface {
	Connect() error
	Disconnect() error
	Info()
	Read() (chan string, error)
}

// Destination is an interface that must be implemented to
// flow data out of the pipeline.
type Destination interface {
	Connect() error
	Disconnect() error
	Info()
	Write(message string) error
}

type stat struct {
	count uint64
}

// Flow connects to source and destination and then launches a
// goroutine to read from `src` and write to `dest`.
//
// A transformer is optional and can be used to to transform
// data read from `src` before writing it to `dest`.
//
// Example:
//  transform := transformer.JSON{
//      Append: map[string]interface{}{
//          "timestamp": func() interface{} { return time.Now() },
//      },
//  }
func Flow(src Source, transformer transform.Transformer, dest Destination) {
	// interrupt channel for OS signals
	interrupt := make(chan os.Signal, 1)
	// register interrupt channel to receive SIGINT and SIGKILL
	signal.Notify(interrupt, os.Interrupt, os.Kill)

	// Connect
	swissFunc.Retry(src.Connect, interrupt)
	swissFunc.Retry(dest.Connect, interrupt)

	log.Info("Source is: ", reflect.TypeOf(src))
	src.Info()
	log.Info("Destination is: ", reflect.TypeOf(dest))
	dest.Info()
	if transformer != nil {
		transformer.Info()
	}

	// do something!
	var stat stat
	go func() {
		channel, err := src.Read()
		if err != nil {
			log.Fatal("src.Read(): ", err)
		}

		log.Info("Flowing data...")

		for message := range channel {
			if transformer != nil {
				var err error
				message, err = transformer.Transform(message)
				if err != nil {
					log.Error("Failed to transform message: ", err)
				}
			}
			err := dest.Write(message)
			if err == nil {
				stat.count++
			} else {
				log.Error(err)
			}
		}
	}()

	// Interrupt received
	<-interrupt
	log.Info("Interrupt received.")
	log.Info("Sent messages: ", stat.count)

	// Disconnect
	src.Disconnect()
	dest.Disconnect()
}
