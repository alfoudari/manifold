package stream

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

type WebSocket struct {
	URL    string
	Header http.Header
	Args   map[string]string
	conn   *websocket.Conn
	disc   chan bool // disconnect signal
	swap   chan bool // conn swap signal
}

func (w *WebSocket) Info() {
	log.Info("URL: ", w.URL)
}

func (w *WebSocket) Connect() (err error) {
	w.conn = getConnection(w)

	if _, ok := w.Args["reconnect_every"]; ok {
		log.Info("Got `reconnect_every` arg, launching `Reonnect` goroutine...")
		// create channels
		w.disc = make(chan bool)
		// launch goroutine
		go w.Reconnect()
	}

	return
}

func getConnection(w *WebSocket) (conn *websocket.Conn) {
	// connect to a websocket connection
	log.Info("Establishing websocket connection...")
	conn, _, err := websocket.DefaultDialer.Dial(w.URL, w.Header)
	if err != nil {
		log.Fatal("Dial: ", err)
	}
	log.Info("Websocket connection established.")
	return
}

func (w *WebSocket) Disconnect() (err error) {
	// send a disconnect signal
	select {
	case w.disc <- true:
		log.Info("sent disc signal")
	default:
		log.Info("no disc channel receivers")
	}

	if w.conn == nil {
		log.Warn("WebSocket.Disconnect(): conn is nil")
		return
	}

	err = closeWebsocket(w.conn)
	return
}

func (w *WebSocket) Reconnect() (err error) {
	// wait
	reconnectEvery := 1 * time.Minute
	// override reconnectEvery from Args
	if val, ok := w.Args["reconnect_every"]; ok {
		if n, err := strconv.Atoi(val); err == nil {
			reconnectEvery = time.Duration(n)
		} else {
			log.Errorln(val, "is not an integer.")
		}
	}
	log.Info("Reconnecting every ", reconnectEvery)

	prevConn := w.conn
	for {
		time.Sleep(reconnectEvery)
		// check for a disconnect signal, quit if received
		select {
		case <-w.disc:
			log.Warn("Received disconnect signal")
			return
		default:
			log.Info("Swapping connections...")
			// connect to a websocket connection
			w.conn = getConnection(w)
			log.Info("prevConn: ", prevConn.UnderlyingConn())
			log.Info("w.conn: ", w.conn.UnderlyingConn())
			closeWebsocket(prevConn)
			prevConn = w.conn
		}
	}
}

func (w *WebSocket) Write(message string) (err error) {
	err = w.conn.WriteMessage(websocket.TextMessage, []byte(message))
	if err != nil {
		log.Error(err)
	}
	return
}

func (w *WebSocket) Read() (channel chan string, err error) {
	channel = make(chan string)
	go func() {
		for {
			log.Trace("Read() iteration, w.conn: ", w.conn.UnderlyingConn())
			_, messageBytes, err := w.conn.ReadMessage()
			log.Debug("ReadMessage() done")
			if err != nil {
				log.Error("ReadMessage() error: ", err)
				continue
			}
			log.Debug("trying to push messageBytes into channel")
			channel <- string(messageBytes)
			log.Debug("channel <- messageBytes successful")
		}
	}()
	return
}

func closeWebsocket(conn *websocket.Conn) (err error) {
	if conn == nil {
		err = errors.New("conn is nil")
		return
	}

	log.Info("Closing websocket connection...")
	err = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	if err != nil {
		log.Error("Websocket write close error: ", err)
		return
	}
	log.Info("Websocket connection closed.")

	return
}
