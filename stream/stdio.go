package stream

import (
	"strings"
	"os"
	"bufio"
	"fmt"
)

type Stdio struct{}

func (s *Stdio) Connect() (err error) {
	return nil
}

func (s *Stdio) Disconnect() (err error) {
	return nil
}

func (s *Stdio) Write(message string) (err error) {
	_, err = fmt.Println(message)
	return
}

func (s *Stdio) Read() (channel chan string, err error) {
	channel = make(chan string)
	go func() {
		for {
			reader := bufio.NewReader(os.Stdin)
			text, _ := reader.ReadString('\n')
			text = strings.TrimSuffix(text, "\n")
			channel <- string(text)
		}
	}()
	return
}

func (s *Stdio) Info() {
	return
}
