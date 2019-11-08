package stream

import "fmt"

type Stdout struct{}

func (s *Stdout) Connect() (err error) {
	return nil
}

func (s *Stdout) Disconnect() (err error) {
	return nil
}

func (s *Stdout) Write(message string) (err error) {
	_, err = fmt.Println(message)
	return
}

func (s *Stdout) Info() {
	return
}
