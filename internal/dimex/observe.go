package dimex

// This file is used only by the visualization (cmd/viz). NewObserved creates
// the same module as New, but wraps the Perfect Link in a transport that
// reports every message send and delivery and, optionally, delays
// deliveries so the message flow is visible. The algorithm does not change.

import (
	"encoding/json"
	"time"

	"t1sd/internal/pl"
)

// LinkEvent is a message send or delivery observed on the link.
type LinkEvent struct {
	Time      time.Time
	Delivered bool // false: send; true: delivery to the module
	From, To  int
	Msg       Message
}

// NewObserved creates a module like New and calls observe on every message
// send and delivery. Each received message is delivered to the module
// latency after it arrives from the network, in arrival order, which
// preserves the FIFO order of each channel. observe is called from several
// goroutines and must return quickly.
func NewObserved(cfg Config, latency time.Duration, observe func(LinkEvent)) (*Module, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	link, err := pl.Listen(cfg.Addresses[cfg.ID], cfg.Logger)
	if err != nil {
		return nil, err
	}
	t := &observedTransport{
		link:    link,
		self:    cfg.ID,
		ids:     make(map[string]int, len(cfg.Addresses)),
		latency: latency,
		observe: observe,
		ind:     make(chan pl.Message),
		quit:    make(chan struct{}),
	}
	for i, a := range cfg.Addresses {
		t.ids[a] = i
	}
	go t.forward()
	m := newModule(cfg, t)
	go m.run()
	return m, nil
}

type observedTransport struct {
	link    *pl.Link
	self    int
	ids     map[string]int
	latency time.Duration
	observe func(LinkEvent)
	ind     chan pl.Message
	quit    chan struct{}
}

func (t *observedTransport) Send(to string, payload []byte) error {
	// The send is reported before it reaches the network, so that no
	// delivery is observed before the corresponding send.
	t.emit(false, t.self, t.ids[to], payload)
	return t.link.Send(to, payload)
}

func (t *observedTransport) Ind() <-chan pl.Message { return t.ind }

func (t *observedTransport) Close() error {
	close(t.quit)
	return t.link.Close()
}

// forward passes the messages from the link to the module, each one latency
// after it arrives. The intermediate queue prevents the delay of one message
// from adding to the delay of the next.
func (t *observedTransport) forward() {
	type pending struct {
		due time.Time
		msg pl.Message
	}
	queue := make(chan pending, 4096)
	go func() {
		for {
			select {
			case m := <-t.link.Ind():
				select {
				case queue <- pending{time.Now().Add(t.latency), m}:
				case <-t.quit:
					return
				}
			case <-t.quit:
				return
			}
		}
	}()

	for {
		var p pending
		select {
		case p = <-queue:
		case <-t.quit:
			return
		}
		if wait := time.Until(p.due); wait > 0 {
			select {
			case <-time.After(wait):
			case <-t.quit:
				return
			}
		}
		if from, ok := t.ids[p.msg.From]; ok {
			t.emit(true, from, t.self, p.msg.Payload)
		}
		select {
		case t.ind <- p.msg:
		case <-t.quit:
			return
		}
	}
}

func (t *observedTransport) emit(delivered bool, from, to int, payload []byte) {
	var msg Message
	if json.Unmarshal(payload, &msg) != nil {
		return
	}
	t.observe(LinkEvent{Time: time.Now(), Delivered: delivered, From: from, To: to, Msg: msg})
}
