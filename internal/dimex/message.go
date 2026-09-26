package dimex

import (
	"fmt"
)

// Kind is the type of a message between DIMEX modules.
type Kind string

const (
	KindReq    Kind = "req"    // request to enter the CS, with a timestamp
	KindOK     Kind = "ok"     // reply that authorizes entry
	KindMarker Kind = "marker" // Chandy-Lamport snapshot marker
)

// Message is the format of the messages exchanged between DIMEX modules,
// encoded as JSON over the Perfect Link. KindReq and KindOK messages also
// appear in snapshots, as channel contents.
type Message struct {
	Kind Kind `json:"kind"`
	TS   int  `json:"ts,omitempty"`   // KindReq: request timestamp
	Snap int  `json:"snap,omitempty"` // KindMarker: snapshot id
}

// State is the mutual exclusion state of a process.
type State int

const (
	NoMX   State = iota // does not want the CS
	WantMX              // requested the CS and waits for replies
	InMX                // is in the CS
)

var stateNames = [...]string{NoMX: "noMX", WantMX: "wantMX", InMX: "inMX"}

func (s State) String() string {
	if s < 0 || int(s) >= len(stateNames) {
		return fmt.Sprintf("State(%d)", int(s))
	}
	return stateNames[s]
}

// MarshalText writes the state by name in snapshots.
func (s State) MarshalText() ([]byte, error) {
	if s < 0 || int(s) >= len(stateNames) {
		return nil, fmt.Errorf("dimex: invalid state %d", int(s))
	}
	return []byte(stateNames[s]), nil
}

func (s *State) UnmarshalText(b []byte) error {
	for i, name := range stateNames {
		if string(b) == name {
			*s = State(i)
			return nil
		}
	}
	return fmt.Errorf("dimex: unknown state %q", b)
}

// Fault selects an injected fault in the rule that decides between replying
// to a request and deferring it. It shows that the snapshot analysis detects
// property violations.
type Fault int

const (
	NoFault Fault = iota
	// FaultMutex: in WantMX, replies to every request without comparing
	// priorities. Two processes that request at the same time enter the CS
	// together (violates DMX2).
	FaultMutex
	// FaultBlock: in WantMX, defers every request without comparing
	// priorities. Two processes that request at the same time wait for each
	// other forever (violates DMX1).
	FaultBlock
)

var faultNames = [...]string{NoFault: "none", FaultMutex: "mutex", FaultBlock: "block"}

func (f Fault) String() string {
	if f < 0 || int(f) >= len(faultNames) {
		return fmt.Sprintf("Fault(%d)", int(f))
	}
	return faultNames[f]
}

// ParseFault converts "none", "mutex" or "block" to a Fault.
func ParseFault(s string) (Fault, error) {
	for i, name := range faultNames {
		if s == name {
			return Fault(i), nil
		}
	}
	return NoFault, fmt.Errorf("dimex: unknown fault %q (use none, mutex or block)", s)
}

// Before reports whether the request (ts1, id1) has priority over
// (ts2, id2): the lower timestamp wins, and a tie goes to the lower id.
func Before(ts1, id1, ts2, id2 int) bool {
	if ts1 != ts2 {
		return ts1 < ts2
	}
	return id1 < id2
}
