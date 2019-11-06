package stream

import (
	"net/http"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

type WebSocket struct {
	URL    string
	Header http.Header
	Args   map[string]string
	conn   *websocket.Conn
}

func (w *WebSocket) Connect() (err error) {
	// connect to a websocket connection
	log.Info("Establishing websocket connection...")
	w.conn, _, err = websocket.DefaultDialer.Dial(w.URL, w.Header)
	if err != nil {
		log.Fatal("Dial: ", err)
	}
	log.Info("Websocket connection established.")
	return
}

func (w *WebSocket) Disconnect() (err error) {
	log.Info("Closing websocket connection...")
	err = w.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	if err != nil {
		log.Error("Websocket write close error: ", err)
		return
	}
	log.Info("Websocket connection closed.")

	return
}

func (w *WebSocket) Read() (channel chan string, err error) {
	channel = make(chan string)
	go func() {
		for {
			_, messageBytes, err := w.conn.ReadMessage()
			if err != nil {
				log.Error("ReadMessage() error: ", err)
				return
			}
			channel <- string(messageBytes)
		}
	}()
	return
}
