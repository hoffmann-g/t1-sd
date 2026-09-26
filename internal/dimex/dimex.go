// Package dimex implements distributed mutual exclusion (the Ricart-Agrawala
// algorithm, with a Lamport logical clock) extended with the Chandy-Lamport
// snapshot, over the Perfect Link from package pl.
//
// Application interface:
//
//	Lock     blocks until access to the critical section (CS) is granted.
//	Unlock   releases the CS.
//	Snapshot starts a snapshot and waits until its local state is complete.
//
// Properties:
//
//	DMX1 (no postponement and no deadlock): every call to Lock returns,
//	     provided every process that enters the CS calls Unlock.
//	DMX2 (mutual exclusion): if Lock returned at p, Lock does not return at
//	     any other process before p calls Unlock.
//
// Concurrency: all state of both algorithms belongs to a single goroutine
// (run), which handles one event at a time: a request from the application
// or a message delivered by the link. Each event is atomic, as in the
// specification presented in class, and the state needs no locks.
package dimex

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"

	"t1sd/internal/pl"
)

// ErrClosed is returned by the methods of Module after Close.
var ErrClosed = errors.New("dimex: module closed")

// Config configures a Module.
type Config struct {
	ID        int      // index of this process in Addresses
	Addresses []string // address of every process, in the same order everywhere
	Fault     Fault    // injected fault; NoFault for the correct algorithm

	// OnSnapshot is called when the local state of a snapshot is complete:
	// the process variables and the state of every incoming channel. It runs
	// on the module goroutine, so it must return quickly and must not call
	// methods of Module. It may be nil.
	OnSnapshot func(Snapshot)

	Logger *log.Logger // may be nil
}

// transport is the part of the Perfect Link that the module uses. The unit
// tests replace it with an in-memory implementation.
type transport interface {
	Send(to string, payload []byte) error
	Ind() <-chan pl.Message
	Close() error
}

// Module is the DIMEX module of one process.
//
// Lock and Unlock must be called by one goroutine at a time (the
// application), alternately. Snapshot may be called from any goroutine,
// including while another goroutine is blocked in Lock.
type Module struct {
	id     int
	addrs  []string
	ids    map[string]int // address → id
	link   transport
	fault  Fault
	onSnap func(Snapshot)
	logger *log.Logger

	app       chan *appRequest
	quit      chan struct{}
	done      chan struct{} // closed when run returns
	closeOnce sync.Once

	// Ricart-Agrawala.
	st      State
	waiting []bool        // waiting[q]: the reply to q's request was deferred
	clock   int           // Lamport logical clock
	reqTS   int           // timestamp of the current request
	resps   int           // replies received for the current request
	granted chan struct{} // closed when the current request is granted

	// Chandy-Lamport.
	snaps    map[int]*snapshotRun // snapshots with channels still being recorded
	lastSnap int                  // last snapshot id started here
}

type appOp int

const (
	opLock appOp = iota
	opUnlock
	opSnapshot
)

type appRequest struct {
	op   appOp
	done chan struct{} // closed when the request is served
	snap int           // opSnapshot: assigned id, valid after done
}

// New creates the module, opens the Perfect Link at address Addresses[ID]
// and starts handling events.
func New(cfg Config) (*Module, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	link, err := pl.Listen(cfg.Addresses[cfg.ID], cfg.Logger)
	if err != nil {
		return nil, err
	}
	m := newModule(cfg, link)
	go m.run()
	return m, nil
}

func (cfg Config) validate() error {
	n := len(cfg.Addresses)
	if n == 0 {
		return errors.New("dimex: empty address list")
	}
	if cfg.ID < 0 || cfg.ID >= n {
		return fmt.Errorf("dimex: id %d out of range [0, %d)", cfg.ID, n)
	}
	seen := make(map[string]bool, n)
	for _, a := range cfg.Addresses {
		if seen[a] {
			return fmt.Errorf("dimex: duplicate address %s", a)
		}
		seen[a] = true
	}
	return nil
}

func newModule(cfg Config, link transport) *Module {
	logger := cfg.Logger
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	ids := make(map[string]int, len(cfg.Addresses))
	for i, a := range cfg.Addresses {
		ids[a] = i
	}
	return &Module{
		id:      cfg.ID,
		addrs:   cfg.Addresses,
		ids:     ids,
		link:    link,
		fault:   cfg.Fault,
		onSnap:  cfg.OnSnapshot,
		logger:  logger,
		app:     make(chan *appRequest),
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
		st:      NoMX,
		waiting: make([]bool, len(cfg.Addresses)),
		snaps:   make(map[int]*snapshotRun),
	}
}

// Lock blocks until this process may enter the CS.
func (m *Module) Lock() error {
	_, err := m.do(opLock)
	return err
}

// Unlock releases the CS. It returns after the deferred replies are sent.
func (m *Module) Unlock() error {
	_, err := m.do(opUnlock)
	return err
}

// Snapshot starts a snapshot and returns its id when its local state is
// complete (the marker from every other process was received). Ids are
// 1, 2, 3...; the scheme assumes a single initiator process.
func (m *Module) Snapshot() (int, error) {
	return m.do(opSnapshot)
}

// Close shuts down the module and the Perfect Link. Calls blocked in Lock or
// Snapshot return ErrClosed.
func (m *Module) Close() error {
	m.closeOnce.Do(func() { close(m.quit) })
	<-m.done
	return m.link.Close()
}

func (m *Module) do(op appOp) (int, error) {
	req := &appRequest{op: op, done: make(chan struct{})}
	select {
	case m.app <- req:
	case <-m.done:
		return 0, ErrClosed
	}
	select {
	case <-req.done:
		return req.snap, nil
	case <-m.done:
		return 0, ErrClosed
	}
}

// run is the event loop: the only goroutine that accesses the state.
func (m *Module) run() {
	defer close(m.done)
	for {
		select {
		case req := <-m.app:
			m.handleApp(req)
		case d := <-m.link.Ind():
			m.handleDelivery(d)
		case <-m.quit:
			return
		}
	}
}

func (m *Module) handleApp(req *appRequest) {
	switch req.op {
	case opLock:
		m.handleLock(req.done)
	case opUnlock:
		m.handleUnlock()
		close(req.done)
	case opSnapshot:
		req.snap = m.startSnapshot(req.done)
	}
}

func (m *Module) handleDelivery(d pl.Message) {
	from, ok := m.ids[d.From]
	if !ok || from == m.id {
		m.logger.Printf("dimex: ignoring message from unknown sender %q", d.From)
		return
	}
	var msg Message
	if err := json.Unmarshal(d.Payload, &msg); err != nil {
		m.logger.Printf("dimex: invalid message from p%d: %v", from, err)
		return
	}
	switch msg.Kind {
	case KindReq:
		m.recordInTransit(from, msg)
		m.handleReq(from, msg.TS)
	case KindOK:
		m.recordInTransit(from, msg)
		m.handleOK(from)
	case KindMarker:
		m.handleMarker(from, msg.Snap)
	default:
		m.logger.Printf("dimex: message of unknown kind %q from p%d", msg.Kind, from)
	}
}

// ---------------------------------------------------------------------------
// Ricart-Agrawala
// ---------------------------------------------------------------------------

// handleLock: upon [dmx, Entry]
//
//	lts++; myTs := lts; resps := 0
//	for all q ≠ p: trigger [pl, Send | q, [reqEntry, myTs]]
//	state := wantMX
func (m *Module) handleLock(granted chan struct{}) {
	if m.st != NoMX {
		panic(fmt.Sprintf("dimex: Lock called in state %v", m.st))
	}
	m.clock++
	m.reqTS = m.clock
	m.resps = 0
	m.st = WantMX
	m.granted = granted
	m.broadcast(Message{Kind: KindReq, TS: m.reqTS})
	m.checkGranted() // with a single process there are no replies to wait for
}

// handleUnlock: upon [dmx, Exit]
//
//	for all q in waiting: trigger [pl, Send | q, [respOk]]
//	state := noMX; waiting := {}
func (m *Module) handleUnlock() {
	if m.st != InMX {
		panic(fmt.Sprintf("dimex: Unlock called in state %v", m.st))
	}
	m.st = NoMX
	m.resps = 0
	for q, w := range m.waiting {
		if w {
			m.waiting[q] = false
			m.send(q, Message{Kind: KindOK})
		}
	}
}

// handleReq: upon [pl, Deliver | q, [reqEntry, ts]]
//
//	if state == noMX or (state == wantMX and (ts, q) < (myTs, p))
//	    then trigger [pl, Send | q, [respOk]]
//	    else waiting := waiting + q
//	lts := max(lts, ts) + 1
func (m *Module) handleReq(from, ts int) {
	m.clock = max(m.clock, ts) + 1
	if m.shouldDefer(from, ts) {
		m.waiting[from] = true
		return
	}
	m.send(from, Message{Kind: KindOK})
}

// shouldDefer decides whether the request (ts, from) waits until this
// process leaves the CS. The injected faults are inserted here.
func (m *Module) shouldDefer(from, ts int) bool {
	switch m.st {
	case InMX:
		return true
	case WantMX:
		switch m.fault {
		case FaultMutex:
			return false // FAULT: ignores priority and always replies
		case FaultBlock:
			return true // FAULT: ignores priority and always defers
		}
		return Before(m.reqTS, m.id, ts, from)
	default:
		return false
	}
}

// handleOK: upon [pl, Deliver | q, [respOk]]
//
//	resps++
//	if resps == N-1 then trigger [dmx, Deliver | free2Access]; state := inMX
func (m *Module) handleOK(from int) {
	if m.st != WantMX {
		m.logger.Printf("dimex: unexpected reply from p%d in state %v", from, m.st)
		return
	}
	m.resps++
	m.checkGranted()
}

func (m *Module) checkGranted() {
	if m.st == WantMX && m.resps == len(m.addrs)-1 {
		m.st = InMX
		close(m.granted)
		m.granted = nil
	}
}

func (m *Module) send(to int, msg Message) {
	payload, err := json.Marshal(msg)
	if err != nil {
		panic(err) // Message has only serializable fields
	}
	if err := m.link.Send(m.addrs[to], payload); err != nil {
		m.logger.Printf("dimex: send to p%d: %v", to, err)
	}
}

func (m *Module) broadcast(msg Message) {
	for q := range m.addrs {
		if q != m.id {
			m.send(q, msg)
		}
	}
}
