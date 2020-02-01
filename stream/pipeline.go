package stream

import (
	"github.com/abstractpaper/manifold/transform"
	"os"
	"os/signal"
	"reflect"

	log "github.com/sirupsen/logrus"
)

type Source interface {
	Connect() error
	Disconnect() error
	Info()
	Read() (chan string, error)
}

type Destination interface {
	Connect() error
	Disconnect() error
	Info()
	Write(message string) error
}

type stat struct {
	count uint64
}

func Flow(src Source, transformer transform.Transformer, dest Destination) {
	// interrupt channel for OS signals
	interrupt := make(chan os.Signal, 1)
	// register interrupt channel to receive SIGINT and SIGKILL
	signal.Notify(interrupt, os.Interrupt, os.Kill)

	// Connect
	src.Connect()
	dest.Connect()

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
