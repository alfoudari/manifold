package stream

import (
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

// WebSocket represents a websocket connection.
//
// Args:
//   reconnect_every: n
//   Attempt to reconnect every n time is passed.
//
// Example:
//
//   Args: map[string]string{
//       "reconnect_every": strconv.Itoa(int(1 * time.Minute)),
//   }
type WebSocket struct {
	URL    string // URL of websocket connection
	Header http.Header
	Args   map[string]string
	conn   *websocket.Conn // holds connection instance
	swap   chan bool       // conn swap signal
	disc   chan bool       // disconnect signal
	wg     sync.WaitGroup
}

// Info logs the websocket connection information.
func (w *WebSocket) Info() {
	log.Info("URL: ", w.URL)
}

// getConnection attempts to connect the URL in WebSocket
// and return a connection.
func getConnection(w *WebSocket) (conn *websocket.Conn) {
	log.Info("Establishing websocket connection...")
	conn, _, err := websocket.DefaultDialer.Dial(w.URL, w.Header)
	if err != nil {
		log.Fatal("getConnection; websocket.Dial: ", err)
	}
	log.Info("Websocket connection established.")
	return
}

// Connect creates a new connection and launches a go
// routine to reconnect periodically if `reconnect_every` is
// in Args.
func (w *WebSocket) Connect() (err error) {
	w.conn = getConnection(w)
	w.disc = make(chan bool)

	if _, ok := w.Args["reconnect_every"]; ok {
		log.Info("Got `reconnect_every` arg, launching `Reconnect` goroutine...")
		w.wg.Add(1)
		go w.Reconnect()
	}

	return
}

// Disconnect sends a disconnect signal so all go routines
// and interested parties get a notification to clean up,
// then it closes the web socket connection.
func (w *WebSocket) Disconnect() (err error) {
	log.Info("WebSocket: Disconnect()")

	// send a disconnect signal
	if _, ok := w.Args["reconnect_every"]; ok {
		log.Info("Sending disc signal")
		w.disc <- true
	}

	if w.conn == nil {
		log.Warn("WebSocket.Disconnect(): conn is nil")
		return
	}

	err = closeWebsocket(w.conn)

	// wait for go routines to finish
	log.Info("Waiting for goroutines to finish...")
	w.wg.Wait()
	log.Info("Goroutines finished.")

	return
}

// Reconnect runs a loop to swap the websocket connection
// periodically.
//
// The default reconnect period is 1 minute, it is used
// when the value of `reonnect_every` is not readable.
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
		// check for a disconnect signal, quit if received
		select {
		case <-w.disc:
			log.Warn("Reconnect(): Received disconnect signal")
			w.wg.Done()
			return
		case <-time.After(reconnectEvery):
			log.Warn("Swapping connections...")

			// connect to a websocket connection
			w.conn = getConnection(w)
			log.Warn("Connection swapped successfully.")
			log.Trace("prevConn: ", prevConn.UnderlyingConn())
			log.Trace("w.conn: ", w.conn.UnderlyingConn())
			closeWebsocket(prevConn)
			prevConn = w.conn
		}
	}
}

// Write writes `message` (transformed into bytes) to the websocket connection.
func (w *WebSocket) Write(message string) (err error) {
	err = w.conn.WriteMessage(websocket.TextMessage, []byte(message))
	if err != nil {
		log.Error(err)
	}
	return
}

// Read launches a go routine that runs a loop to read from
// the websocket connection.
//
// Read also attempts to get a new connection if reading from
// the existing connection raises an error. This is to avoid
// repeated ReadMessage errors that would panic the process.
// It is useful for cases when the server you are connecting
// to drops the connection from its side. If the connection
// attempt fails, the process exits.
func (w *WebSocket) Read() (channel chan string, err error) {
	channel = make(chan string)
	go func() {
		for {
			log.Trace("Read() iteration, w.conn: ", w.conn.UnderlyingConn())

			_, messageBytes, err := w.conn.ReadMessage()
			log.Debug("ReadMessage() done")
			if err != nil {
				log.Warning("ReadMessage() error: ", err)
				w.conn = getConnection(w)
				log.Warning("Connection swapped successfully.")
				continue
			}

			log.Debug("trying to push messageBytes into channel")
			channel <- string(messageBytes)
			log.Debug("channel <- messageBytes successful")
		}
	}()
	return
}

// closeMessage closes the websocet connection in `conn`.
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
