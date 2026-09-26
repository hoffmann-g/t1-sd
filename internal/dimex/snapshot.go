package dimex

import "slices"

// Snapshot is the local state of a process in a snapshot: the algorithm
// variables at the moment of recording and the state of each incoming
// channel.
type Snapshot struct {
	ID      int    `json:"id"`
	Process int    `json:"process"`
	State   State  `json:"state"`
	Clock   int    `json:"clock"`
	ReqTS   int    `json:"reqTs"`
	Resps   int    `json:"resps"`
	Waiting []bool `json:"waiting"`

	// Channels[q] holds the messages in transit on channel q → this process:
	// those received from q after the local state was recorded and before the
	// marker from q. There is one entry (possibly empty) for each other
	// process.
	Channels map[int][]Message `json:"channels"`
}

// snapshotRun is a snapshot whose local state is already recorded but that
// still waits for the marker on some incoming channel.
type snapshotRun struct {
	rec  Snapshot
	open map[int]bool  // incoming channels still being recorded
	done chan struct{} // initiator only: closed on completion
}

// startSnapshot starts a snapshot at the application's request (initiator
// rule) and returns the assigned id.
func (m *Module) startSnapshot(done chan struct{}) int {
	m.lastSnap++
	run := m.recordLocal(m.lastSnap)
	run.done = done
	m.finishIfComplete(run)
	return run.rec.ID
}

// handleMarker: rule for receiving the marker from from.
//
//	if the local state of the snapshot is not yet recorded:
//	    record the local state; channel from is empty;
//	    start recording the other channels and send the marker to all
//	else:
//	    stop recording channel from
func (m *Module) handleMarker(from, id int) {
	run, ok := m.snaps[id]
	if !ok {
		run = m.recordLocal(id)
	}
	delete(run.open, from)
	m.finishIfComplete(run)
}

// recordLocal records the local state of snapshot id, starts recording
// every incoming channel and sends the marker to every other process.
func (m *Module) recordLocal(id int) *snapshotRun {
	run := &snapshotRun{
		rec: Snapshot{
			ID:       id,
			Process:  m.id,
			State:    m.st,
			Clock:    m.clock,
			ReqTS:    m.reqTS,
			Resps:    m.resps,
			Waiting:  slices.Clone(m.waiting),
			Channels: make(map[int][]Message, len(m.addrs)-1),
		},
		open: make(map[int]bool, len(m.addrs)-1),
	}
	for q := range m.addrs {
		if q != m.id {
			run.open[q] = true
			run.rec.Channels[q] = []Message{}
		}
	}
	m.snaps[id] = run
	m.broadcast(Message{Kind: KindMarker, Snap: id})
	return run
}

// recordInTransit appends msg, received from from, to the state of channel
// from → this process in every snapshot that is still recording that
// channel. It must be called before the message is handled.
func (m *Module) recordInTransit(from int, msg Message) {
	for _, run := range m.snaps {
		if run.open[from] {
			run.rec.Channels[from] = append(run.rec.Channels[from], msg)
		}
	}
}

func (m *Module) finishIfComplete(run *snapshotRun) {
	if len(run.open) > 0 {
		return
	}
	delete(m.snaps, run.rec.ID)
	if m.onSnap != nil {
		m.onSnap(run.rec)
	}
	if run.done != nil {
		close(run.done)
	}
}
