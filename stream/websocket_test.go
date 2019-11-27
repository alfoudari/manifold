package stream

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

var upgrader = websocket.Upgrader{}

func TestWebSocket_Connect(t *testing.T) {
	// Create test server with the echo handler.
	server := httptest.NewServer(http.HandlerFunc(echo))
	fmt.Println("Test server created")
	defer server.Close()

	src := WebSocket{
		URL:    "ws" + strings.TrimPrefix(server.URL, "http"),
		Header: http.Header{},
	}

	src.Connect()
}

func TestWebSocket_Disonnect(t *testing.T) {
	// Create test server with the echo handler.
	server := httptest.NewServer(http.HandlerFunc(echo))
	fmt.Println("Test server created")
	defer server.Close()

	src := WebSocket{
		URL:    "ws" + strings.TrimPrefix(server.URL, "http"),
		Header: http.Header{},
	}

	src.Connect()
	src.Disconnect()
}

func TestWebSocket_ReadWrite(t *testing.T) {
	// Create test server with the echo handler.
	server := httptest.NewServer(http.HandlerFunc(echo))
	fmt.Println("Test server created")
	defer server.Close()

	src := WebSocket{
		URL:    "ws" + strings.TrimPrefix(server.URL, "http"),
		Header: http.Header{},
	}

	src.Connect()

	msg, err := src.Read()
	for i := 0; i < 5; i++ {
		src.Write("echo")
		if err != nil {
			log.Info("couldn't write")
			t.Fatal(err)
		}
		fmt.Println("msg: ", <-msg)
	}
}

func TestWebSocket_Reconnect(t *testing.T) {
	// Create test server with the echo handler.
	server := httptest.NewServer(http.HandlerFunc(echo))
	fmt.Println("Test server created")
	defer server.Close()

	src := WebSocket{
		URL:    "ws" + strings.TrimPrefix(server.URL, "http"),
		Header: http.Header{},
		Args: map[string]string{
			"reconnect_every": strconv.Itoa(int(1 * time.Second)),
		},
	}
	src.Connect()

	msg, err := src.Read()
	for i := 0; i < 30; i++ {
		src.Write("echo")
		if err != nil {
			log.Info("couldn't write")
			t.Fatal(err)
		}
		fmt.Println("msg: ", <-msg)
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(3 * time.Second)

	log.Warn("Trying to disconnect...")
	src.Disconnect()
}

func echo(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer c.Close()
	for {
		mt, message, err := c.ReadMessage()
		if err != nil {
			break
		}
		err = c.WriteMessage(mt, message)
		if err != nil {
			break
		}
	}
}
