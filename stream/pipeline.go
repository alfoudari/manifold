package stream

import (
	"os"
	"os/signal"
	"reflect"
	"time"

	"github.com/abstractpaper/manifold/transform"

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
	retry(src.Connect, interrupt)
	retry(dest.Connect, interrupt)

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

func retry(f func() error, interrupt chan os.Signal) {
	var sleep time.Duration = 2
	var retryTimestamp time.Time = time.Now()
	for {
		select {
		case <-interrupt:
			log.Warn("Interrupt/kill signal received, quitting.")
			os.Exit(1)
		default:
			// call f() if the current time passed retryTimestamp
			if time.Now().After(retryTimestamp) {
				err := f()
				if err != nil {
					log.Error(err)

					// couldn't connect, retry.
					log.Infof("Retrying in %d seconds", sleep)
					retryTimestamp = time.Now().Add(sleep * time.Second)

					// increase exponentially, hard cap at ~ 1 minute (64 seconds).
					if sleep < 64 {
						sleep = sleep * 2
					}
				} else {
					return
				}
			}

			// loop every second, check for signals and timeline in each iteration.
			time.Sleep(1 * time.Second)
		}
	}
}
