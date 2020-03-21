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
	conn   *websocket.Conn      // holds connection instance
	swap   chan bool            // conn swap signal
	disc   map[string]chan bool // disconnect signal map (creates multiple  channels)
	wg     sync.WaitGroup
}

// Info logs the websocket connection information.
func (w *WebSocket) Info() {
	log.Info("URL: ", w.URL)
}

// Connect creates a new connection and launches a go
// routine to reconnect periodically if `reconnect_every` is
// in Args.
func (w *WebSocket) Connect() (err error) {
	err = w.newConnection()
	w.swap = make(chan bool)
	w.disc = map[string]chan bool{
		"reconnect": make(chan bool),
		"read":      make(chan bool),
	}

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
	log.Info("WebSocket: Disconnect() started")

	// close connection
	if w.conn != nil {
		err = closeWebSocket(w.conn)
		w.conn = nil
	}

	// send a disconnect signal
	if _, ok := w.Args["reconnect_every"]; ok {
		log.Info("Sending disc signal to all channels.")
		for name, c := range w.disc {
			log.Infof("Sending to channel %s...", name)
			c <- true
			log.Infof("Sent disc signal to channel %s", name)
		}
	}

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
		case <-w.disc["reconnect"]:
			log.Warn("Reconnect(): Received disconnect signal")
			w.wg.Done()
			return
		case <-time.After(reconnectEvery):
			log.Warn("WebSocket.Reconnect(): Swapping connections...")

			// send a swapping started signal
			w.swap <- true

			// connect to a websocket connection
			err := w.newConnection()
			if err != nil {
				continue
			}

			log.Warn("WebSocket.Reconnect(): Connection swapped successfully.")
			log.Trace("prevConn: ", prevConn.UnderlyingConn())
			log.Trace("w.conn: ", w.conn.UnderlyingConn())
			closeWebSocket(prevConn)
			prevConn = w.conn

			// send a swapping stopped signal
			w.swap <- false
		}
	}
}

// Write writes `message` (transformed into bytes) to the websocket connection.
func (w *WebSocket) Write(message string) (err error) {
	if w.conn == nil {
		return errors.New("w.conn is nil")
	}

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
	w.wg.Add(1)
	go func() {
		for {
			select {
			case <-w.disc["read"]:
				log.Warn("Read(): Received disconnect signal")
				w.wg.Done()
				return
			case swap := <-w.swap:
				// swap signal received:
				log.Tracef("swap signal received: %t", swap)
				// if swap started (true), wait for a false signal
				if swap {
					_ = <-w.swap
					// swap is done
				}
				// continue to next iteration
				continue
			default:
				// no disc or swap signal received
				if w.conn != nil {
					log.Trace("Read() iteration, w.conn: ", w.conn.UnderlyingConn())

					_, messageBytes, err := w.conn.ReadMessage()
					log.Debug("ReadMessage() done")
					if err != nil {
						log.Warning("ReadMessage() error: ", err)

						if w.conn != nil {
							// ReadMessage() should not receive an error as Disconnect() nullifies w.conn
							w.newConnection()
						}

						continue
					}

					log.Debug("trying to push messageBytes into channel")
					channel <- string(messageBytes)
					log.Debug("channel <- messageBytes successful")
				}
			}
		}
	}()
	return
}

// newConnection attempts to connect the URL in WebSocket
// and return a connection.
func (w *WebSocket) newConnection() (err error) {
	log.Info("Establishing websocket connection...")
	var conn *websocket.Conn
	conn, _, err = websocket.DefaultDialer.Dial(w.URL, w.Header)
	w.conn = conn
	if err == nil {
		log.Info("Websocket connection established.")
	} else {
		log.Error("WebSocket.newConnection: websocket.Dial: ", err)
	}
	return
}

// closeWebSocket closes the websocet connection in `conn`.
func closeWebSocket(conn *websocket.Conn) (err error) {
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
