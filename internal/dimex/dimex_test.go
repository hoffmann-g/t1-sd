package dimex

// Unit tests for the event handlers. The module is built without a network
// and without the run goroutine: each test calls the handlers one by one and
// inspects the messages the module sent to the transport.

import (
	"encoding/json"
	"fmt"
	"testing"

	"t1sd/internal/pl"
)

type sentMsg struct {
	to  int
	msg Message
}

type fakeTransport struct {
	ids  map[string]int
	sent []sentMsg
}

func (f *fakeTransport) Send(to string, payload []byte) error {
	var m Message
	if err := json.Unmarshal(payload, &m); err != nil {
		return err
	}
	f.sent = append(f.sent, sentMsg{f.ids[to], m})
	return nil
}

func (f *fakeTransport) Ind() <-chan pl.Message { return nil }
func (f *fakeTransport) Close() error           { return nil }

func addr(p int) string { return fmt.Sprintf("p%d", p) }

type harness struct {
	*Module
	tr    *fakeTransport
	snaps []Snapshot
}

func newHarness(n, id int, fault Fault) *harness {
	addrs := make([]string, n)
	ids := map[string]int{}
	for i := range addrs {
		addrs[i] = addr(i)
		ids[addrs[i]] = i
	}
	h := &harness{tr: &fakeTransport{ids: ids}}
	h.Module = newModule(Config{ID: id, Addresses: addrs, Fault: fault,
		OnSnapshot: func(s Snapshot) { h.snaps = append(h.snaps, s) }}, h.tr)
	return h
}

// drain returns and discards the messages sent so far.
func (h *harness) drain() []sentMsg {
	s := h.tr.sent
	h.tr.sent = nil
	return s
}

func (h *harness) deliver(from int, msg Message) {
	payload, _ := json.Marshal(msg)
	h.handleDelivery(pl.Message{From: addr(from), Payload: payload})
}

// lock starts a request and returns the channel that is closed when the
// request is granted.
func (h *harness) lock() chan struct{} {
	done := make(chan struct{})
	h.handleLock(done)
	return done
}

func closed(c chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

func req(ts int) Message { return Message{Kind: KindReq, TS: ts} }

var ok = Message{Kind: KindOK}

func marker(id int) Message { return Message{Kind: KindMarker, Snap: id} }

func TestBefore(t *testing.T) {
	cases := []struct {
		ts1, id1, ts2, id2 int
		want               bool
	}{
		{1, 0, 2, 1, true},  // lower timestamp wins
		{3, 0, 2, 1, false}, // higher timestamp loses
		{5, 0, 5, 1, true},  // tie: lower id wins
		{5, 2, 5, 1, false}, // tie: higher id loses
	}
	for _, c := range cases {
		if got := Before(c.ts1, c.id1, c.ts2, c.id2); got != c.want {
			t.Errorf("Before(%d,%d,%d,%d) = %v, want %v", c.ts1, c.id1, c.ts2, c.id2, got, c.want)
		}
	}
}

// Lock: the clock is incremented and a request with that timestamp goes to
// the N-1 other processes.
func TestLockBroadcastsRequest(t *testing.T) {
	h := newHarness(4, 1, NoFault)
	h.clock = 7
	g := h.lock()

	if h.st != WantMX || h.reqTS != 8 || h.clock != 8 {
		t.Fatalf("st=%v reqTS=%d clock=%d, want wantMX 8 8", h.st, h.reqTS, h.clock)
	}
	to := map[int]bool{}
	for _, s := range h.drain() {
		if s.msg != req(8) {
			t.Fatalf("message %+v, want req(8)", s.msg)
		}
		to[s.to] = true
	}
	if len(to) != 3 || to[1] {
		t.Fatalf("destinations %v, want {0, 2, 3}", to)
	}
	if closed(g) {
		t.Fatal("access granted before the replies")
	}
}

func TestSingleProcessEntersImmediately(t *testing.T) {
	h := newHarness(1, 0, NoFault)
	if g := h.lock(); !closed(g) || h.st != InMX {
		t.Fatal("single process did not enter the CS immediately")
	}
}

func TestGrantAfterAllResponses(t *testing.T) {
	h := newHarness(3, 0, NoFault)
	g := h.lock()
	h.deliver(1, ok)
	if closed(g) || h.st != WantMX {
		t.Fatal("access granted with 1 of 2 replies")
	}
	h.deliver(2, ok)
	if !closed(g) || h.st != InMX {
		t.Fatal("access not granted with 2 of 2 replies")
	}
}

func TestUnexpectedOKIgnored(t *testing.T) {
	for _, st := range []State{NoMX, InMX} {
		h := newHarness(3, 0, NoFault)
		h.st = st
		h.deliver(1, ok)
		if h.st != st || h.resps != 0 {
			t.Fatalf("ok in %v changed the state", st)
		}
	}
}

func TestRequestWhenNotInterested(t *testing.T) {
	h := newHarness(3, 0, NoFault)
	h.deliver(2, req(4))
	if s := h.drain(); len(s) != 1 || s[0] != (sentMsg{2, ok}) {
		t.Fatalf("sent %v, want ok to p2", s)
	}
	if h.waiting[2] {
		t.Fatal("waiting set in noMX")
	}
}

func TestRequestWhenInCSIsDeferred(t *testing.T) {
	h := newHarness(3, 0, NoFault)
	h.st = InMX
	h.deliver(1, req(1))
	if s := h.drain(); len(s) != 0 {
		t.Fatalf("replied while in the CS: %v", s)
	}
	if !h.waiting[1] {
		t.Fatal("waiting[1] not set")
	}
}

// In wantMX the decision follows the (ts, id) priority, except with a fault.
func TestConflictWhileWanting(t *testing.T) {
	cases := []struct {
		name         string
		myID, myTS   int
		othID, othTS int
		fault        Fault
		respond      bool
	}{
		{"my ts lower: defer", 0, 2, 1, 5, NoFault, false},
		{"my ts higher: reply", 0, 5, 1, 2, NoFault, true},
		{"tie, my id lower: defer", 0, 3, 2, 3, NoFault, false},
		{"tie, my id higher: reply", 2, 3, 0, 3, NoFault, true},
		{"FaultMutex replies even with priority", 0, 2, 1, 5, FaultMutex, true},
		{"FaultBlock defers even without priority", 0, 5, 1, 2, FaultBlock, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(3, c.myID, c.fault)
			h.st, h.reqTS, h.clock = WantMX, c.myTS, c.myTS
			h.deliver(c.othID, req(c.othTS))

			responded := len(h.drain()) == 1
			if responded != c.respond {
				t.Fatalf("replied=%v, want %v", responded, c.respond)
			}
			if h.waiting[c.othID] == c.respond {
				t.Fatalf("waiting[%d]=%v inconsistent with the reply", c.othID, h.waiting[c.othID])
			}
			if h.reqTS != c.myTS {
				t.Fatal("received request changed the timestamp of this process's request")
			}
		})
	}
}

// Faults change the decision only in wantMX: in noMX and inMX the behavior
// is correct.
func TestFaultsOnlyAffectWantMX(t *testing.T) {
	for _, f := range []Fault{FaultMutex, FaultBlock} {
		h := newHarness(2, 0, f)
		h.deliver(1, req(1))
		if len(h.drain()) != 1 {
			t.Errorf("%v: did not reply in noMX", f)
		}
		h.st = InMX
		h.deliver(1, req(2))
		if len(h.drain()) != 0 || !h.waiting[1] {
			t.Errorf("%v: did not defer in inMX", f)
		}
	}
}

func TestLamportClock(t *testing.T) {
	h := newHarness(2, 0, NoFault)
	h.clock = 3
	h.deliver(1, req(10))
	if h.clock != 11 {
		t.Fatalf("clock = %d, want 11", h.clock)
	}
	h.deliver(1, req(2))
	if h.clock != 12 {
		t.Fatalf("clock = %d, want 12", h.clock)
	}
}

func TestUnlockAnswersDeferred(t *testing.T) {
	h := newHarness(4, 0, NoFault)
	h.st, h.resps = InMX, 3
	h.waiting[1], h.waiting[3] = true, true
	h.handleUnlock()

	if h.st != NoMX || h.resps != 0 {
		t.Fatalf("st=%v resps=%d after Unlock", h.st, h.resps)
	}
	for q, w := range h.waiting {
		if w {
			t.Fatalf("waiting[%d] still set", q)
		}
	}
	s := h.drain()
	if len(s) != 2 || s[0] != (sentMsg{1, ok}) || s[1] != (sentMsg{3, ok}) {
		t.Fatalf("sent %v, want ok to p1 and p3", s)
	}
}

func TestInvalidMessagesIgnored(t *testing.T) {
	h := newHarness(3, 0, NoFault)
	h.handleDelivery(pl.Message{From: "unknown", Payload: []byte(`{"kind":"req","ts":1}`)})
	h.handleDelivery(pl.Message{From: addr(0), Payload: []byte(`{"kind":"req","ts":1}`)}) // from itself
	h.handleDelivery(pl.Message{From: addr(1), Payload: []byte(`not json`)})
	h.handleDelivery(pl.Message{From: addr(1), Payload: []byte(`{"kind":"xyz"}`)})
	if s := h.drain(); len(s) != 0 {
		t.Fatalf("replied to an invalid message: %v", s)
	}
}

func TestMisusePanics(t *testing.T) {
	for name, f := range map[string]func(h *harness){
		"Unlock outside the CS": func(h *harness) { h.handleUnlock() },
		"double Lock":           func(h *harness) { h.lock(); h.lock() },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("incorrect use did not panic")
				}
			}()
			f(newHarness(2, 0, NoFault))
		})
	}
}

// Two processes request at the same time. The one with higher priority
// enters first, and the other enters only after the Unlock.
func TestTwoProcessConflict(t *testing.T) {
	p0, p1 := newHarness(2, 0, NoFault), newHarness(2, 1, NoFault)
	g0, g1 := p0.lock(), p1.lock() // both with ts=1: p0 has priority
	req0, req1 := p0.drain()[0].msg, p1.drain()[0].msg
	p1.deliver(0, req0)
	p0.deliver(1, req1)

	toP0, toP1 := p1.drain(), p0.drain()
	if len(toP0) != 1 || len(toP1) != 0 {
		t.Fatalf("p1 sent %v and p0 sent %v; want only p1 to reply", toP0, toP1)
	}
	p0.deliver(1, toP0[0].msg)
	if !closed(g0) || closed(g1) {
		t.Fatal("want p0 in the CS and p1 waiting")
	}

	p0.handleUnlock()
	p1.deliver(0, p0.drain()[0].msg)
	if !closed(g1) {
		t.Fatal("p1 did not enter after the Unlock of p0")
	}
}

// ---------------------------------------------------------------------------
// Chandy-Lamport
// ---------------------------------------------------------------------------

// Initiator: records its state, sends the marker to all and completes when
// it receives the marker from all. Without traffic, the channels are empty.
func TestSnapshotInitiator(t *testing.T) {
	h := newHarness(3, 0, NoFault)
	h.clock = 4
	done := make(chan struct{})
	id := h.startSnapshot(done)

	if id != 1 {
		t.Fatalf("id = %d, want 1", id)
	}
	s := h.drain()
	if len(s) != 2 || s[0] != (sentMsg{1, marker(1)}) || s[1] != (sentMsg{2, marker(1)}) {
		t.Fatalf("sent %v, want marker to p1 and p2", s)
	}
	h.deliver(1, marker(1))
	if closed(done) || len(h.snaps) != 0 {
		t.Fatal("snapshot completed before the marker from p2")
	}
	h.deliver(2, marker(1))
	if !closed(done) || len(h.snaps) != 1 {
		t.Fatal("snapshot did not complete after all markers")
	}
	got := h.snaps[0]
	if got.ID != 1 || got.Process != 0 || got.State != NoMX || got.Clock != 4 {
		t.Fatalf("recorded state %+v", got)
	}
	for q, msgs := range got.Channels {
		if len(msgs) != 0 {
			t.Fatalf("channel p%d not empty: %v", q, msgs)
		}
	}
	if next := h.startSnapshot(make(chan struct{})); next != 2 {
		t.Fatalf("second id = %d, want 2", next)
	}
}

// Non-initiator: the first marker records the state. The channel of the
// marker's sender is empty. Messages on other channels received before their
// marker become part of the channel state.
func TestSnapshotRecordsChannelState(t *testing.T) {
	h := newHarness(3, 1, NoFault)
	g := h.lock() // p1 in wantMX, request ts=1
	h.drain()

	h.deliver(0, marker(7)) // records the local state (wantMX, resps=0)
	if s := h.drain(); len(s) != 2 || s[0].msg != marker(7) {
		t.Fatalf("did not forward the marker: %v", s)
	}
	h.deliver(0, ok)     // after the marker from p0: not in transit
	h.deliver(2, ok)     // before the marker from p2: in transit
	h.deliver(2, req(9)) // same
	h.deliver(2, marker(7))
	h.deliver(2, ok) // after all markers

	if len(h.snaps) != 1 {
		t.Fatalf("%d complete snapshots, want 1", len(h.snaps))
	}
	s := h.snaps[0]
	if s.State != WantMX || s.Resps != 0 || s.ReqTS != 1 {
		t.Fatalf("recorded state %+v, want wantMX resps=0 reqTs=1", s)
	}
	if len(s.Channels[0]) != 0 {
		t.Fatalf("channel p0→p1 = %v, want empty", s.Channels[0])
	}
	if want := []Message{ok, req(9)}; fmt.Sprint(s.Channels[2]) != fmt.Sprint(want) {
		t.Fatalf("channel p2→p1 = %v, want %v", s.Channels[2], want)
	}
	// Recording does not interfere with the algorithm: both replies counted.
	if !closed(g) {
		t.Fatal("replies received during the snapshot were not handled")
	}
}

// The recorded state is a copy: later changes do not modify it.
func TestSnapshotCopiesState(t *testing.T) {
	h := newHarness(2, 0, NoFault)
	h.st = InMX
	h.waiting[1] = true
	h.startSnapshot(make(chan struct{}))
	h.handleUnlock()
	h.deliver(1, marker(1))

	if s := h.snaps[0]; !s.Waiting[1] || s.State != InMX {
		t.Fatalf("recorded state changed: %+v", s)
	}
}

// Overlapping snapshots are independent.
func TestConcurrentSnapshots(t *testing.T) {
	h := newHarness(2, 1, NoFault)
	h.deliver(0, marker(1))
	h.deliver(0, marker(2))
	if len(h.snaps) != 2 || h.snaps[0].ID != 1 || h.snaps[1].ID != 2 {
		t.Fatalf("snapshots %+v", h.snaps)
	}

	h3 := newHarness(3, 0, NoFault)
	h3.deliver(1, marker(1)) // snapshot 1 records the channel from p2
	h3.deliver(2, req(5))    // in transit in snapshot 1
	h3.deliver(2, marker(2)) // snapshot 2 records the channel from p1
	h3.deliver(1, ok)        // p0 in noMX: in transit only in snapshot 2
	h3.deliver(2, marker(1))
	h3.deliver(1, marker(2))

	if len(h3.snaps) != 2 {
		t.Fatalf("%d complete snapshots, want 2", len(h3.snaps))
	}
	s1, s2 := h3.snaps[0], h3.snaps[1]
	if len(s1.Channels[1]) != 0 || fmt.Sprint(s1.Channels[2]) != fmt.Sprint([]Message{req(5)}) {
		t.Fatalf("snapshot 1 channels %v", s1.Channels)
	}
	if fmt.Sprint(s2.Channels[1]) != fmt.Sprint([]Message{ok}) || len(s2.Channels[2]) != 0 {
		t.Fatalf("snapshot 2 channels %v", s2.Channels)
	}
}

func TestStateTextRoundTrip(t *testing.T) {
	for _, s := range []State{NoMX, WantMX, InMX} {
		b, err := s.MarshalText()
		if err != nil {
			t.Fatal(err)
		}
		var back State
		if err := back.UnmarshalText(b); err != nil || back != s {
			t.Fatalf("%v → %q → %v (%v)", s, b, back, err)
		}
	}
	var s State
	if err := s.UnmarshalText([]byte("xyz")); err == nil {
		t.Fatal("unknown state accepted")
	}
}

func TestParseFault(t *testing.T) {
	for _, f := range []Fault{NoFault, FaultMutex, FaultBlock} {
		if got, err := ParseFault(f.String()); err != nil || got != f {
			t.Fatalf("ParseFault(%q) = %v, %v", f.String(), got, err)
		}
	}
	if _, err := ParseFault("other"); err == nil {
		t.Fatal("unknown fault accepted")
	}
}

func TestConfigValidation(t *testing.T) {
	for name, cfg := range map[string]Config{
		"no addresses":      {},
		"negative id":       {ID: -1, Addresses: []string{"a"}},
		"id too large":      {ID: 2, Addresses: []string{"a", "b"}},
		"duplicate address": {ID: 0, Addresses: []string{"a", "a"}},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: New accepted an invalid configuration", name)
		}
	}
}
