// Package invariant evaluates global snapshots of DIMEX.
//
// A global snapshot groups, for a given id, the local state recorded by each
// process. Each invariant is a function that receives the global snapshot and
// reports whether the property holds in it. With the correct algorithm, all
// of them hold in every consistent snapshot.
package invariant

import (
	"fmt"
	"slices"
	"strings"

	"t1sd/internal/dimex"
)

// Global is a complete global snapshot.
type Global struct {
	ID    int
	Procs []dimex.Snapshot // Procs[p] is the local state of p
}

// Invariant is a property that must hold in every global snapshot.
type Invariant struct {
	Name  string
	Desc  string
	Check func(Global) bool
}

// All lists the invariants evaluated by the tool, in report order.
var All = []Invariant{
	{"Inv1", "at most one process in the CS", Inv1},
	{"Inv2", "if no process wants the CS, there is no waiting and no message in transit", Inv2},
	{"Inv3", "if p deferred q, p is in the CS or wants the CS", Inv3},
	{"Inv4", "if p deferred q, q wants the CS", Inv4},
	{"Inv5", "if q wants the CS, replies received + oks in transit + waitings for q + requests from q in transit = N-1", Inv5},
	{"Inv6", "no two processes defer each other", Inv6},
	{"Inv7", "reply counter consistent with the state", Inv7},
	{"Inv8", "p in wantMX defers q only if the request of p has priority", Inv8},
	{"Inv9", "a process that does not want the CS has no request or reply in transit", Inv9},
}

// Inv1: at most one process in the CS (mutual exclusion, DMX2).
func Inv1(g Global) bool {
	in := 0
	for _, p := range g.Procs {
		if p.State == dimex.InMX {
			in++
		}
	}
	return in <= 1
}

// Inv2: if all processes are in noMX, no waiting entry is set and no channel
// has messages.
func Inv2(g Global) bool {
	for _, p := range g.Procs {
		if p.State != dimex.NoMX {
			return true
		}
	}
	for _, p := range g.Procs {
		if slices.Contains(p.Waiting, true) {
			return false
		}
		for _, msgs := range p.Channels {
			if len(msgs) > 0 {
				return false
			}
		}
	}
	return true
}

// Inv3: if q is marked as waiting in p, p is in the CS or wants the CS.
func Inv3(g Global) bool {
	for _, p := range g.Procs {
		if slices.Contains(p.Waiting, true) && p.State == dimex.NoMX {
			return false
		}
	}
	return true
}

// Inv4: if q is marked as waiting in p, q wants the CS (it is waiting for the
// reply from p).
func Inv4(g Global) bool {
	for _, p := range g.Procs {
		for q, w := range p.Waiting {
			if w && g.Procs[q].State != dimex.WantMX {
				return false
			}
		}
	}
	return true
}

// Inv5: if q wants the CS, each of the other N-1 processes is in exactly one
// of these situations: the request from q is in transit to it; it deferred q;
// its reply is in transit to q; q already received its reply.
// Therefore: resps(q) + oks in transit to q + waitings for q + requests from
// q in transit = N-1.
func Inv5(g Global) bool {
	n := len(g.Procs)
	for q, s := range g.Procs {
		if s.State != dimex.WantMX {
			continue
		}
		total := s.Resps
		for p := range g.Procs {
			if p == q {
				continue
			}
			total += g.inTransit(p, q, dimex.KindOK)
			total += g.inTransit(q, p, dimex.KindReq)
			if g.Procs[p].Waiting[q] {
				total++
			}
		}
		if total != n-1 {
			return false
		}
	}
	return true
}

// Inv6: there are no p and q with p deferring q and q deferring p. That would
// be a circular wait: neither would reply and both would wait forever.
func Inv6(g Global) bool {
	for p := range g.Procs {
		for q := p + 1; q < len(g.Procs); q++ {
			if g.Procs[p].Waiting[q] && g.Procs[q].Waiting[p] {
				return false
			}
		}
	}
	return true
}

// Inv7: the reply counter is consistent with the state: 0 in noMX, fewer
// than N-1 in wantMX (with N-1 the process would already have entered) and
// N-1 in inMX.
func Inv7(g Global) bool {
	n := len(g.Procs)
	for _, p := range g.Procs {
		switch p.State {
		case dimex.NoMX:
			if p.Resps != 0 {
				return false
			}
		case dimex.WantMX:
			if p.Resps >= n-1 {
				return false
			}
		case dimex.InMX:
			if p.Resps != n-1 {
				return false
			}
		}
	}
	return true
}

// Inv8: a process in wantMX defers the request from q only if its own
// request has priority, that is, a smaller (ts, id).
func Inv8(g Global) bool {
	for p, s := range g.Procs {
		if s.State != dimex.WantMX {
			continue
		}
		for q, w := range s.Waiting {
			if w && !dimex.Before(s.ReqTS, p, g.Procs[q].ReqTS, q) {
				return false
			}
		}
	}
	return true
}

// Inv9: if q does not want the CS, there is no request from q and no reply
// to q in transit. In inMX q already received all replies, and each of them
// comes after the corresponding request; in noMX there is no pending
// request.
func Inv9(g Global) bool {
	for q, s := range g.Procs {
		if s.State == dimex.WantMX {
			continue
		}
		for p := range g.Procs {
			if p != q && (g.inTransit(q, p, dimex.KindReq) > 0 || g.inTransit(p, q, dimex.KindOK) > 0) {
				return false
			}
		}
	}
	return true
}

// inTransit counts the messages of type kind in transit from process from to
// process to.
func (g Global) inTransit(from, to int, kind dimex.Kind) int {
	n := 0
	for _, m := range g.Procs[to].Channels[from] {
		if m.Kind == kind {
			n++
		}
	}
	return n
}

// Violated returns the invariants of All that do not hold in g.
func (g Global) Violated() []Invariant {
	var out []Invariant
	for _, inv := range All {
		if !inv.Check(g) {
			out = append(out, inv)
		}
	}
	return out
}

// String describes the snapshot, one process per line.
func (g Global) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "snapshot %d\n", g.ID)
	for p, s := range g.Procs {
		fmt.Fprintf(&b, "  p%d: %-6v reqTs=%-5d resps=%d waiting=%v", p, s.State, s.ReqTS, s.Resps, waitingList(s.Waiting))
		for q := range g.Procs {
			if msgs := s.Channels[q]; len(msgs) > 0 {
				fmt.Fprintf(&b, " chan p%d→p%d=%v", q, p, kinds(msgs))
			}
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func waitingList(w []bool) []int {
	out := []int{}
	for q, v := range w {
		if v {
			out = append(out, q)
		}
	}
	return out
}

func kinds(msgs []dimex.Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = string(m.Kind)
		if m.Kind == dimex.KindReq {
			out[i] += fmt.Sprintf("(%d)", m.TS)
		}
	}
	return out
}
