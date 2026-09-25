// Package pl implements the Perfect Point-to-Point Link (Cachin, Guerraoui and
// Rodrigues, "Introduction to Reliable and Secure Distributed Programming",
// section 2.4) over TCP.
//
// Properties, assuming processes that do not crash:
//
//	PL1 (reliable delivery): if p sends m to q, q delivers m.
//	PL2 (no duplication): no message is delivered more than once.
//	PL3 (no creation): if q delivers m with sender p, m was sent by p.
//
// In addition, messages from the same sender are delivered in the order they
// were sent (FIFO). The Chandy-Lamport snapshot depends on this property.
//
// Implementation: each (source, destination) pair uses a single TCP
// connection, opened by the source. While the connection is open, TCP
// guarantees delivery, ordering and absence of duplication. The dial is
// retried until the destination accepts, so processes can be started in any
// order. A single connection per pair is what guarantees FIFO: there are
// never two concurrent connections to the same destination. If an established
// connection drops, which the model excludes, the error is logged and Send
// returns an error for that destination from then on.
//
// Wire format: each message is a 4-byte length header (big-endian) followed
// by the payload. The first message on each connection is the listen address
// of the dialing side, used as the sender (From) in deliveries.
package pl

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// MaxMessageSize is the largest payload accepted in a message.
const MaxMessageSize = 1 << 20

// ErrClosed is returned by Send after Close.
var ErrClosed = errors.New("pl: link closed")

// Message is a message delivered by the link.
type Message struct {
	From    string // listen address of the sender
	Payload []byte
}

// Link is the Perfect Link of a process, bound to its listen address.
// Its methods can be called from multiple goroutines.
type Link struct {
	addr   string
	ln     net.Listener
	ind    chan Message
	logger *log.Logger
	quit   chan struct{}
	wg     sync.WaitGroup

	mu     sync.Mutex
	closed bool
	peers  map[string]*peer
	conns  map[net.Conn]struct{} // open connections, closed in Close
}

// peer holds the outgoing queue for a destination. One goroutine (sendLoop)
// drains the queue into the connection to the destination.
type peer struct {
	to    string
	ready chan struct{} // signals that the queue received messages

	mu    sync.Mutex
	queue [][]byte
	err   error // connection lost: new messages are rejected
}

// Listen creates the link and starts accepting connections on addr. The
// address addr is also what other processes see as the sender. logger may be
// nil.
func Listen(addr string, logger *log.Logger) (*Link, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("pl: listen on %s: %w", addr, err)
	}
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	l := &Link{
		addr:   addr,
		ln:     ln,
		ind:    make(chan Message, 64),
		logger: logger,
		quit:   make(chan struct{}),
		peers:  make(map[string]*peer),
		conns:  make(map[net.Conn]struct{}),
	}
	l.wg.Add(1)
	go l.acceptLoop()
	return l, nil
}

// Addr returns the listen address of the link.
func (l *Link) Addr() string { return l.addr }

// Ind returns the indication channel, where received messages are delivered.
// The channel is not closed by Close.
func (l *Link) Ind() <-chan Message { return l.ind }

// Send sends payload to the process with listen address to. Send does not
// wait for the network: the message goes into an unbounded queue, drained by
// a goroutine dedicated to the destination. A caller of Send is therefore
// never blocked by a process that is also trying to send to it.
func (l *Link) Send(to string, payload []byte) error {
	if len(payload) > MaxMessageSize {
		return fmt.Errorf("pl: message of %d bytes exceeds the limit of %d", len(payload), MaxMessageSize)
	}
	p := l.peer(to)
	if p == nil {
		return ErrClosed
	}
	p.mu.Lock()
	if p.err != nil {
		p.mu.Unlock()
		return p.err
	}
	p.queue = append(p.queue, append([]byte(nil), payload...))
	p.mu.Unlock()

	select {
	case p.ready <- struct{}{}:
	default: // a signal was already pending
	}
	return nil
}

// Close closes the listener and all connections, and waits for the link
// goroutines to finish. Messages still in the queue are discarded.
func (l *Link) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	close(l.quit)
	err := l.ln.Close()
	for c := range l.conns {
		c.Close()
	}
	l.mu.Unlock()

	l.wg.Wait()
	return err
}

// peer returns the queue for destination to, creating it (and its send
// goroutine) on first use. It returns nil if the link is closed.
func (l *Link) peer(to string) *peer {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	p, ok := l.peers[to]
	if !ok {
		p = &peer{to: to, ready: make(chan struct{}, 1)}
		l.peers[to] = p
		l.wg.Add(1)
		go l.sendLoop(p)
	}
	return p
}

func (l *Link) sendLoop(p *peer) {
	defer l.wg.Done()

	conn, err := l.dial(p.to)
	if err != nil {
		return // link closed before connecting
	}
	defer l.untrack(conn)

	w := bufio.NewWriter(conn)
	err = writeFrame(w, []byte(l.addr))
	for err == nil {
		batch, ok := p.take(l.quit)
		if !ok {
			return
		}
		for _, m := range batch {
			if err = writeFrame(w, m); err != nil {
				break
			}
		}
		if err == nil {
			err = w.Flush()
		}
	}

	if l.isClosed() {
		return
	}
	l.logger.Printf("pl: connection to %s lost: %v", p.to, err)
	p.mu.Lock()
	p.err = fmt.Errorf("pl: connection to %s lost: %w", p.to, err)
	p.queue = nil
	p.mu.Unlock()
}

// take waits until the queue has messages and returns all of them at once.
// It returns false if quit is closed.
func (p *peer) take(quit <-chan struct{}) ([][]byte, bool) {
	for {
		p.mu.Lock()
		batch := p.queue
		p.queue = nil
		p.mu.Unlock()
		if len(batch) > 0 {
			return batch, true
		}
		select {
		case <-p.ready:
		case <-quit:
			return nil, false
		}
	}
}

// dial tries to connect to to until it succeeds or the link is closed.
func (l *Link) dial(to string) (net.Conn, error) {
	delay := 10 * time.Millisecond
	for {
		conn, err := net.DialTimeout("tcp", to, time.Second)
		if err == nil {
			if !l.track(conn) {
				conn.Close()
				return nil, ErrClosed
			}
			return conn, nil
		}
		l.logger.Printf("pl: dial %s: %v (retrying in %v)", to, err, delay)
		select {
		case <-l.quit:
			return nil, ErrClosed
		case <-time.After(delay):
		}
		delay = min(2*delay, time.Second)
	}
}

func (l *Link) acceptLoop() {
	defer l.wg.Done()
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			if l.isClosed() {
				return
			}
			l.logger.Printf("pl: accept: %v", err)
			time.Sleep(10 * time.Millisecond) // avoids a busy loop on a persistent error
			continue
		}
		if !l.track(conn) {
			conn.Close()
			return
		}
		l.wg.Add(1)
		go l.receive(conn)
	}
}

// receive reads the messages from a connection and delivers them in order.
func (l *Link) receive(conn net.Conn) {
	defer l.wg.Done()
	defer l.untrack(conn)

	r := bufio.NewReader(conn)
	hello, err := readFrame(r)
	if err != nil {
		if !errors.Is(err, io.EOF) && !l.isClosed() {
			l.logger.Printf("pl: handshake from %s: %v", conn.RemoteAddr(), err)
		}
		return
	}
	from := string(hello)

	for {
		payload, err := readFrame(r)
		if err != nil {
			if !errors.Is(err, io.EOF) && !l.isClosed() {
				l.logger.Printf("pl: read from %s: %v", from, err)
			}
			return
		}
		select {
		case l.ind <- Message{From: from, Payload: payload}:
		case <-l.quit:
			return
		}
	}
}

func (l *Link) track(c net.Conn) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return false
	}
	l.conns[c] = struct{}{}
	return true
}

func (l *Link) untrack(c net.Conn) {
	l.mu.Lock()
	delete(l.conns, c)
	l.mu.Unlock()
	c.Close()
}

func (l *Link) isClosed() bool {
	select {
	case <-l.quit:
		return true
	default:
		return false
	}
}

func writeFrame(w io.Writer, payload []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func readFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxMessageSize {
		return nil, fmt.Errorf("message of %d bytes exceeds the limit of %d", n, MaxMessageSize)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}
