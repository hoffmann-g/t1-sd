package dimex_test

// System tests: N real DIMEX modules, each with its own Perfect Link on a
// localhost port, accessing a shared resource without pauses.
//
//	DMX1: every Lock returns within the deadline.
//	DMX2: there are never two processes in the CS at the same time.
//
// The snapshot tests run the system with successive snapshots and evaluate
// each global snapshot against the invariants from package invariant, with
// the correct algorithm and with the injected faults.

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"t1sd/internal/dimex"
	"t1sd/internal/invariant"
)

const lockTimeout = 15 * time.Second

type cluster struct {
	mods []*dimex.Module

	mu    sync.Mutex
	snaps []dimex.Snapshot
}

func startCluster(t *testing.T, n int, fault dimex.Fault) *cluster {
	t.Helper()
	addrs := make([]string, n)
	for i := range addrs {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs[i] = ln.Addr().String()
		ln.Close()
	}
	c := &cluster{}
	for i := range addrs {
		m, err := dimex.New(dimex.Config{
			ID:        i,
			Addresses: addrs,
			Fault:     fault,
			OnSnapshot: func(s dimex.Snapshot) {
				c.mu.Lock()
				c.snaps = append(c.snaps, s)
				c.mu.Unlock()
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		c.mods = append(c.mods, m)
	}
	t.Cleanup(func() {
		for _, m := range c.mods {
			m.Close()
		}
	})
	return c
}

// report assembles the global snapshots recorded so far.
func (c *cluster) report(t *testing.T) *invariant.Report {
	t.Helper()
	c.mu.Lock()
	recs := append([]dimex.Snapshot(nil), c.snaps...)
	c.mu.Unlock()
	rep, err := invariant.Assemble(recs)
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

// workload runs the test application: each process makes `rounds` accesses
// to the CS and, inside it, writes "|" and "." to the shared file. rounds < 0
// repeats until stop is closed. It tracks the file contents and the number
// of times a process entered while another was already inside.
type workload struct {
	file       *os.File
	path       string
	inCS       atomic.Int32
	violations atomic.Int32
	wg         sync.WaitGroup
	stop       chan struct{}
}

func startWorkload(t *testing.T, mods []*dimex.Module, rounds int) *workload {
	t.Helper()
	w := &workload{path: filepath.Join(t.TempDir(), "mxOUT.txt"), stop: make(chan struct{})}
	f, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	w.file = f
	t.Cleanup(func() { f.Close() })

	for _, m := range mods {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for r := 0; rounds < 0 || r < rounds; r++ {
				select {
				case <-w.stop:
					return
				default:
				}
				if m.Lock() != nil {
					return // module closed
				}
				if w.inCS.Add(1) != 1 {
					w.violations.Add(1)
				}
				f.WriteString("|")
				f.WriteString(".")
				w.inCS.Add(-1)
				if m.Unlock() != nil {
					return
				}
			}
		}()
	}
	return w
}

// wait waits for the workload to finish. It fails if the workload does not
// finish within the deadline (DMX1).
func (w *workload) wait(t *testing.T) {
	t.Helper()
	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(lockTimeout):
		t.Fatalf("DMX1 violated: processes did not finish within %v (deadlock)", lockTimeout)
	}
}

func (w *workload) checkMutex(t *testing.T, accesses int) {
	t.Helper()
	data, err := os.ReadFile(w.path)
	if err != nil {
		t.Fatal(err)
	}
	content := string(data)
	if v := w.violations.Load(); v != 0 {
		t.Errorf("DMX2 violated: %d entries with another process in the CS", v)
	}
	if c := strings.Count(content, "||"); c != 0 {
		t.Errorf("DMX2 violated: %d occurrences of \"||\"", c)
	}
	if c := strings.Count(content, ".."); c != 0 {
		t.Errorf("DMX2 violated: %d occurrences of \"..\"", c)
	}
	if want := strings.Repeat("|.", accesses); content != want {
		t.Errorf("file has %d bytes, want %d in the \"|.\" pattern", len(content), len(want))
	}
}

func TestMutualExclusion(t *testing.T) {
	for _, c := range []struct {
		name      string
		n, rounds int
	}{{"2proc", 2, 1000}, {"3proc", 3, 500}, {"5proc", 5, 300}} {
		t.Run(c.name, func(t *testing.T) {
			cl := startCluster(t, c.n, dimex.NoFault)
			w := startWorkload(t, cl.mods, c.rounds)
			w.wait(t)
			w.checkMutex(t, c.n*c.rounds)
		})
	}
}

// While p0 is in the CS, the Lock of p1 does not return. After the Unlock,
// it returns.
func TestLockBlocksUntilUnlock(t *testing.T) {
	cl := startCluster(t, 3, dimex.NoFault)
	p0, p1 := cl.mods[0], cl.mods[1]
	if err := p0.Lock(); err != nil {
		t.Fatal(err)
	}

	acquired := make(chan struct{})
	go func() { p1.Lock(); close(acquired) }()
	select {
	case <-acquired:
		t.Fatal("DMX2 violated: p1 entered the CS with p0 inside")
	case <-time.After(300 * time.Millisecond):
	}

	p0.Unlock()
	select {
	case <-acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("DMX1 violated: p1 did not enter after the Unlock of p0")
	}
}

// A process that does not request the CS keeps replying to the others.
func TestIdleProcessAnswers(t *testing.T) {
	cl := startCluster(t, 3, dimex.NoFault)
	w := startWorkload(t, cl.mods[1:], 500)
	w.wait(t)
	w.checkMutex(t, 2*500)
}

// With every process requesting continuously, every process gets to enter
// (no starvation).
func TestNoStarvation(t *testing.T) {
	cl := startCluster(t, 4, dimex.NoFault)
	counts := make([]atomic.Int32, len(cl.mods))
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i, m := range cl.mods {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				m.Lock()
				counts[i].Add(1)
				m.Unlock()
			}
		}()
	}
	time.Sleep(time.Second)
	close(stop)
	wg.Wait()
	for i := range counts {
		if counts[i].Load() == 0 {
			t.Errorf("p%d never entered the CS", i)
		}
	}
}

// takeSnapshots makes p0 start n successive snapshots, concurrently with the
// use of the CS. Every 50 snapshots it calls stop and returns if stop
// returns true.
func takeSnapshots(t *testing.T, m *dimex.Module, n int, stop func() bool) {
	t.Helper()
	for i := 0; i < n; i++ {
		if i%50 == 0 && stop() {
			return
		}
		if _, err := m.Snapshot(); err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
	}
}

// With the correct algorithm, every invariant holds in every snapshot.
func TestSnapshotsSatisfyInvariants(t *testing.T) {
	cl := startCluster(t, 3, dimex.NoFault)
	w := startWorkload(t, cl.mods, -1)
	takeSnapshots(t, cl.mods[0], 500, func() bool { return false })
	close(w.stop)
	w.wait(t)

	rep := cl.report(t)
	if len(rep.Globals) < 500 {
		t.Fatalf("%d complete snapshots, want 500", len(rep.Globals))
	}
	states := map[string]int{}
	for _, g := range rep.Globals {
		for _, inv := range g.Violated() {
			t.Errorf("%s violated: %s\n%v", inv.Name, inv.Desc, g)
		}
		for _, p := range g.Procs {
			states[p.State.String()]++
		}
	}
	// The snapshots must capture the system in use, not idle.
	if states["wantMX"] == 0 || states["inMX"] == 0 {
		t.Fatalf("snapshots did not capture the CS in use: %v", states)
	}
	w.checkMutex(t, strings.Count(readFile(t, w.path), "|"))
}

// With FaultMutex, the snapshots detect two processes in the CS (Inv1).
func TestSnapshotsDetectMutexFault(t *testing.T) {
	cl := startCluster(t, 3, dimex.FaultMutex)
	w := startWorkload(t, cl.mods, -1)
	defer close(w.stop)

	found := violationFound(t, cl, "Inv1")
	takeSnapshots(t, cl.mods[0], 5000, found)
	if !found() {
		t.Fatal("no snapshot showed the violation of Inv1")
	}
}

// With FaultBlock, the system deadlocks and the snapshots show a circular
// wait (Inv6) and deferral without priority (Inv8).
func TestSnapshotsDetectBlockFault(t *testing.T) {
	cl := startCluster(t, 3, dimex.FaultBlock)
	w := startWorkload(t, cl.mods, -1)
	defer close(w.stop)

	found6 := violationFound(t, cl, "Inv6")
	found8 := violationFound(t, cl, "Inv8")
	takeSnapshots(t, cl.mods[0], 5000, func() bool { return found6() && found8() })
	if !found6() || !found8() {
		t.Fatalf("violations detected: Inv6=%v Inv8=%v, want both", found6(), found8())
	}
}

// violationFound returns a function that reports whether any snapshot
// completed so far violates the invariant name.
func violationFound(t *testing.T, cl *cluster, name string) func() bool {
	return func() bool {
		for _, g := range cl.report(t).Globals {
			for _, inv := range g.Violated() {
				if inv.Name == name {
					return true
				}
			}
		}
		return false
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
