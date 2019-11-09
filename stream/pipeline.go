package stream

import (
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

func Flow(src Source, dest Destination) {
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

	// do something!
	go func() {
		var stat stat

		channel, _ := src.Read()

		for message := range channel {
			dest.Write(message)

			stat.count++
			log.Println("Sent messages: ", stat.count)
		}
	}()

	// Interrupt received
	<-interrupt
	log.Info("Interrupt received.")

	// Disconnect
	src.Disconnect()
	dest.Disconnect()
}
