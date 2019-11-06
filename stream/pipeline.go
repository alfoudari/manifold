package stream

import (
	"os"
	"os/signal"

	log "github.com/sirupsen/logrus"
)

type Source interface {
	Connect() error
	Disconnect() error
	Read() (string, error)
}

type Destination interface {
	Connect() error
	Disconnect() error
	Write(map[string]string) error
}

func Flow(src Source, dest Destination) {
	// interrupt channel for OS signals
	interrupt := make(chan os.Signal, 1)
	// register interrupt channel to receive SIGINT and SIGKILL
	signal.Notify(interrupt, os.Interrupt, os.Kill)

	// Connect
	src.Connect()
	dest.Connect()

	// do something!
	go func() {
		for {
			message, _ := src.Read()
			dest.Write(map[string]string{
				"exchange": "btcbnb",
				"key":      "trades",
				"message":  message,
			})
		}
	}()

	// Interrupt received
	<-interrupt
	log.Info("Interrupt received.")

	// Disconnect
	src.Disconnect()
	dest.Disconnect()
}
