package pl

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// freeAddr reserves a free port on localhost and returns the address.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func newLink(t *testing.T) *Link {
	t.Helper()
	l, err := Listen(freeAddr(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func receive(t *testing.T, l *Link) Message {
	t.Helper()
	select {
	case m := <-l.Ind():
		return m
	case <-time.After(3 * time.Second):
		t.Fatal("message not delivered within 3s")
		return Message{}
	}
}

func expectNothing(t *testing.T, l *Link) {
	t.Helper()
	select {
	case m := <-l.Ind():
		t.Fatalf("unexpected message: %q from %s", m.Payload, m.From)
	case <-time.After(200 * time.Millisecond):
	}
}

func send(t *testing.T, l *Link, to, msg string) {
	t.Helper()
	if err := l.Send(to, []byte(msg)); err != nil {
		t.Fatalf("Send: %v", err)
	}
}

// PL1 and PL3: the message is delivered, and the sender is the listen address
// of the sending process (not the ephemeral port of the connection).
func TestDeliverWithSenderAddress(t *testing.T) {
	a, b := newLink(t), newLink(t)
	send(t, a, b.Addr(), "hello")

	m := receive(t, b)
	if string(m.Payload) != "hello" {
		t.Fatalf("payload = %q, want %q", m.Payload, "hello")
	}
	if m.From != a.Addr() {
		t.Fatalf("From = %q, want %q", m.From, a.Addr())
	}
}

func TestBidirectional(t *testing.T) {
	a, b := newLink(t), newLink(t)
	send(t, a, b.Addr(), "ping")
	if m := receive(t, b); string(m.Payload) != "ping" {
		t.Fatalf("b received %q", m.Payload)
	}
	send(t, b, a.Addr(), "pong")
	if m := receive(t, a); string(m.Payload) != "pong" || m.From != b.Addr() {
		t.Fatalf("a received %q from %s", m.Payload, m.From)
	}
}

// Messages sent before the destination exists are delivered once it starts:
// processes can be started in any order.
func TestSendBeforeDestinationListens(t *testing.T) {
	a := newLink(t)
	addrB := freeAddr(t)
	send(t, a, addrB, "early")

	time.Sleep(100 * time.Millisecond)
	b, err := Listen(addrB, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	if m := receive(t, b); string(m.Payload) != "early" {
		t.Fatalf("received %q", m.Payload)
	}
}

// PL1, PL2 and FIFO: many messages all arrive, once each, in order.
func TestFIFONoLossNoDuplication(t *testing.T) {
	a, b := newLink(t), newLink(t)
	const n = 5000
	for i := 0; i < n; i++ {
		send(t, a, b.Addr(), fmt.Sprintf("m%d", i))
	}
	for i := 0; i < n; i++ {
		if m, want := receive(t, b), fmt.Sprintf("m%d", i); string(m.Payload) != want {
			t.Fatalf("message %d = %q, want %q", i, m.Payload, want)
		}
	}
	expectNothing(t, b)
}

// Several senders to the same destination: everything arrives and the order
// of each sender is preserved.
func TestManySendersFIFOPerSender(t *testing.T) {
	dst := newLink(t)
	const senders, perSender = 4, 500
	index := map[string]int{}
	for s := 0; s < senders; s++ {
		l := newLink(t)
		index[l.Addr()] = s
		go func() {
			for i := 0; i < perSender; i++ {
				l.Send(dst.Addr(), []byte(fmt.Sprint(i)))
			}
		}()
	}

	next := make([]int, senders)
	for k := 0; k < senders*perSender; k++ {
		m := receive(t, dst)
		s, ok := index[m.From]
		if !ok {
			t.Fatalf("unknown sender %s", m.From)
		}
		if got := string(m.Payload); got != fmt.Sprint(next[s]) {
			t.Fatalf("sender %d: received %s, want %d", s, got, next[s])
		}
		next[s]++
	}
}

// The payload is transmitted without interpretation: empty, binary, '%', large.
func TestPayloadIsOpaque(t *testing.T) {
	a, b := newLink(t), newLink(t)
	payloads := []string{
		"",
		"100% %d %s %v",
		"\x00\x01\xff binary",
		strings.Repeat("x", 10_000),
		strings.Repeat("y", MaxMessageSize),
	}
	for _, p := range payloads {
		send(t, a, b.Addr(), p)
	}
	for i, want := range payloads {
		if m := receive(t, b); string(m.Payload) != want {
			t.Fatalf("message %d: received %d bytes, want %d", i, len(m.Payload), len(want))
		}
	}
}

// Send copies the payload: modifying the slice after sending does not affect
// the delivery.
func TestSendCopiesPayload(t *testing.T) {
	a, b := newLink(t), newLink(t)
	buf := []byte("original")
	if err := a.Send(b.Addr(), buf); err != nil {
		t.Fatal(err)
	}
	copy(buf, "MODIFIED")
	if m := receive(t, b); string(m.Payload) != "original" {
		t.Fatalf("received %q", m.Payload)
	}
}

func TestOversizedMessageRejected(t *testing.T) {
	a, b := newLink(t), newLink(t)
	if err := a.Send(b.Addr(), make([]byte, MaxMessageSize+1)); err == nil {
		t.Fatal("Send accepted a message above the limit")
	}
	expectNothing(t, b)
}

func TestListenOnBusyPortFails(t *testing.T) {
	a := newLink(t)
	if l, err := Listen(a.Addr(), nil); err == nil {
		l.Close()
		t.Fatal("Listen on a busy port did not return an error")
	}
}

// Connections that do not identify themselves (e.g. port scans) are ignored.
func TestIgnoresConnectionWithoutHandshake(t *testing.T) {
	b := newLink(t)
	c, err := net.Dial("tcp", b.Addr())
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	expectNothing(t, b)
}

func TestClose(t *testing.T) {
	a, b := newLink(t), newLink(t)
	send(t, a, b.Addr(), "x")
	receive(t, b)

	done := make(chan error, 1)
	go func() { done <- a.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return")
	}
	if err := a.Send(b.Addr(), []byte("y")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Send after Close = %v, want ErrClosed", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// Close returns even with a pending send to a destination that does not exist.
func TestCloseWhileDialing(t *testing.T) {
	a := newLink(t)
	send(t, a, freeAddr(t), "nobody listening")
	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() { a.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close hung with a dial in progress")
	}
}
