package invariant

import (
	"os"
	"path/filepath"
	"testing"

	"t1sd/internal/dimex"
)

// proc describes the local state of a process in compact form.
type proc struct {
	st      dimex.State
	reqTS   int
	resps   int
	waiting []int                   // deferred processes
	chans   map[int][]dimex.Message // source channel → messages in transit
}

func global(ps ...proc) Global {
	n := len(ps)
	g := Global{ID: 1, Procs: make([]dimex.Snapshot, n)}
	for p, d := range ps {
		s := dimex.Snapshot{ID: 1, Process: p, State: d.st, ReqTS: d.reqTS, Resps: d.resps,
			Waiting: make([]bool, n), Channels: map[int][]dimex.Message{}}
		for _, q := range d.waiting {
			s.Waiting[q] = true
		}
		for q := 0; q < n; q++ {
			if q != p {
				s.Channels[q] = d.chans[q]
			}
		}
		g.Procs[p] = s
	}
	return g
}

var (
	noMX   = dimex.NoMX
	wantMX = dimex.WantMX
	inMX   = dimex.InMX
	ok     = dimex.Message{Kind: dimex.KindOK}
)

func req(ts int) dimex.Message { return dimex.Message{Kind: dimex.KindReq, TS: ts} }

// States reachable by the correct algorithm with 3 processes: no invariant
// may report a violation.
func TestValidGlobals(t *testing.T) {
	cases := map[string]Global{
		"all idle": global(proc{st: noMX}, proc{st: noMX}, proc{st: noMX}),
		"p0 requested, requests in transit": global(
			proc{st: wantMX, reqTS: 1},
			proc{st: noMX, chans: map[int][]dimex.Message{0: {req(1)}}},
			proc{st: noMX, chans: map[int][]dimex.Message{0: {req(1)}}},
		),
		"p0 with one reply received and one in transit": global(
			proc{st: wantMX, reqTS: 1, resps: 1, chans: map[int][]dimex.Message{2: {ok}}},
			proc{st: noMX},
			proc{st: noMX},
		),
		"p0 in the CS deferring p1, p1 with reply from p2": global(
			proc{st: inMX, reqTS: 1, resps: 2, waiting: []int{1}},
			proc{st: wantMX, reqTS: 3, resps: 1},
			proc{st: noMX},
		),
		"p0 and p1 want, p0 has priority and defers p1": global(
			proc{st: wantMX, reqTS: 2, resps: 1, waiting: []int{1}, chans: map[int][]dimex.Message{2: {ok}}},
			proc{st: wantMX, reqTS: 2, resps: 0, chans: map[int][]dimex.Message{2: {ok}}},
			proc{st: noMX},
		),
	}
	for name, g := range cases {
		for _, inv := range g.Violated() {
			t.Errorf("%s: %s reported a violation in a valid state\n%v", name, inv.Name, g)
		}
	}
}

// For each invariant, a state that violates it.
func TestEachInvariantDetectsViolation(t *testing.T) {
	cases := []struct {
		inv Invariant
		g   Global
	}{
		{All[0], global( // Inv1: two in the CS
			proc{st: inMX, resps: 2}, proc{st: inMX, resps: 2}, proc{st: noMX})},
		{All[1], global( // Inv2: all idle with a message in transit
			proc{st: noMX, chans: map[int][]dimex.Message{1: {ok}}}, proc{st: noMX}, proc{st: noMX})},
		{All[2], global( // Inv3: p0 idle deferring p1
			proc{st: noMX, waiting: []int{1}}, proc{st: wantMX, reqTS: 1, resps: 1}, proc{st: noMX})},
		{All[3], global( // Inv4: p0 defers p1, but p1 does not want the CS
			proc{st: inMX, resps: 2, waiting: []int{1}}, proc{st: noMX}, proc{st: noMX})},
		{All[4], global( // Inv5: p1 waits for a reply that exists nowhere
			proc{st: noMX}, proc{st: wantMX, reqTS: 1, resps: 1}, proc{st: noMX})},
		{All[5], global( // Inv6: p0 and p1 defer each other
			proc{st: wantMX, reqTS: 1, resps: 1, waiting: []int{1}},
			proc{st: wantMX, reqTS: 1, resps: 1, waiting: []int{0}},
			proc{st: noMX})},
		{All[6], global( // Inv7: p0 in the CS with a single reply
			proc{st: inMX, resps: 1}, proc{st: noMX}, proc{st: noMX})},
		{All[7], global( // Inv8: p1 defers p0, but the request of p0 has priority
			proc{st: wantMX, reqTS: 1, resps: 1},
			proc{st: wantMX, reqTS: 5, resps: 1, waiting: []int{0}},
			proc{st: noMX})},
		{All[8], global( // Inv9: reply to p0 in transit with p0 idle
			proc{st: noMX, chans: map[int][]dimex.Message{2: {ok}}}, proc{st: wantMX, reqTS: 1, resps: 2}, proc{st: noMX})},
	}
	for _, c := range cases {
		if c.inv.Check(c.g) {
			t.Errorf("%s (%s) did not detect the violation\n%v", c.inv.Name, c.inv.Desc, c.g)
		}
	}
}

func snap(id, p, n int) dimex.Snapshot {
	return dimex.Snapshot{ID: id, Process: p, Waiting: make([]bool, n)}
}

func TestAssemble(t *testing.T) {
	recs := []dimex.Snapshot{
		snap(2, 1, 2), snap(1, 0, 2), snap(2, 0, 2), snap(1, 1, 2), // 1 and 2 complete
		snap(3, 0, 2), // 3 without p1
	}
	rep, err := Assemble(recs)
	if err != nil {
		t.Fatal(err)
	}
	if rep.N != 2 || len(rep.Globals) != 2 || rep.Globals[0].ID != 1 || rep.Globals[1].ID != 2 {
		t.Fatalf("globals %+v", rep.Globals)
	}
	for _, g := range rep.Globals {
		for p, s := range g.Procs {
			if s.Process != p {
				t.Fatalf("snapshot %d: position %d holds p%d", g.ID, p, s.Process)
			}
		}
	}
	if len(rep.Incomplete) != 1 || rep.Incomplete[0] != 3 {
		t.Fatalf("incomplete %v, want [3]", rep.Incomplete)
	}
}

func TestAssembleRejectsInconsistentInput(t *testing.T) {
	for name, recs := range map[string][]dimex.Snapshot{
		"duplicate process":     {snap(1, 0, 2), snap(1, 0, 2)},
		"different N":           {snap(1, 0, 2), snap(1, 1, 3)},
		"process outside [0,N)": {snap(1, 5, 2)},
	} {
		if _, err := Assemble(recs); err == nil {
			t.Errorf("%s: Assemble accepted", name)
		}
	}
}

// Load skips the truncated last line (process killed during a write).
func TestLoadSkipsTruncatedLine(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("p0.jsonl", `{"id":1,"process":0,"state":"noMX","waiting":[false,false],"channels":{"1":[]}}`+"\n"+`{"id":2,"proc`)
	write("p1.jsonl", `{"id":1,"process":1,"state":"inMX","resps":1,"waiting":[false,false],"channels":{"0":[]}}`+"\n")

	rep, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Globals) != 1 || rep.BadLines != 1 {
		t.Fatalf("globals=%d bad lines=%d, want 1 and 1", len(rep.Globals), rep.BadLines)
	}
	if g := rep.Globals[0]; g.Procs[1].State != dimex.InMX || len(g.Violated()) != 0 {
		t.Fatalf("snapshot read incorrectly:\n%v", g)
	}
}

func TestLoadEmptyDir(t *testing.T) {
	if _, err := Load(t.TempDir()); err == nil {
		t.Fatal("Load accepted a directory without files")
	}
}
