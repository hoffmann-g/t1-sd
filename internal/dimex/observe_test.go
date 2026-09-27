package dimex_test

import (
	"net"
	"sync"
	"testing"
	"time"

	"t1sd/internal/dimex"
)

// NewObserved reports every send and every delivery, delays deliveries by
// the configured latency and does not change the behavior of the algorithm.
func TestObservedModule(t *testing.T) {
	const n, latency = 3, 50 * time.Millisecond
	addrs := make([]string, n)
	for i := range addrs {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs[i] = ln.Addr().String()
		ln.Close()
	}

	var mu sync.Mutex
	var events []dimex.LinkEvent
	mods := make([]*dimex.Module, n)
	for i := range mods {
		m, err := dimex.NewObserved(dimex.Config{ID: i, Addresses: addrs}, latency, func(e dimex.LinkEvent) {
			mu.Lock()
			events = append(events, e)
			mu.Unlock()
		})
		if err != nil {
			t.Fatal(err)
		}
		defer m.Close()
		mods[i] = m
	}

	start := time.Now()
	if err := mods[0].Lock(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 2*latency {
		t.Fatalf("Lock returned in %v, want at least %v (request and reply delayed)", elapsed, 2*latency)
	}
	mods[0].Unlock()

	mu.Lock()
	defer mu.Unlock()
	sends := map[[2]int][]dimex.LinkEvent{}
	delivers := map[[2]int][]dimex.LinkEvent{}
	for _, e := range events {
		key := [2]int{e.From, e.To}
		if e.Delivered {
			delivers[key] = append(delivers[key], e)
		} else {
			sends[key] = append(sends[key], e)
		}
	}
	// p0 sends a request to p1 and p2; each one replies to p0.
	for _, key := range [][2]int{{0, 1}, {0, 2}, {1, 0}, {2, 0}} {
		s, d := sends[key], delivers[key]
		if len(s) != 1 || len(d) != 1 {
			t.Fatalf("channel p%d→p%d: %d sends and %d deliveries, want 1 and 1", key[0], key[1], len(s), len(d))
		}
		if s[0].Msg != d[0].Msg {
			t.Fatalf("channel p%d→p%d: sent %+v, delivered %+v", key[0], key[1], s[0].Msg, d[0].Msg)
		}
		if gap := d[0].Time.Sub(s[0].Time); gap < latency {
			t.Fatalf("channel p%d→p%d: delivered %v after the send, want at least %v", key[0], key[1], gap, latency)
		}
	}
	if k := sends[[2]int{0, 1}][0].Msg.Kind; k != dimex.KindReq {
		t.Fatalf("first message from p0 = %s, want req", k)
	}
}
