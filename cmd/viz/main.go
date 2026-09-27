// Command viz shows DiMEx running in a web page: one timeline per process
// with its states (noMX, wantMX, inMX), the messages exchanged (req, ok,
// marker), the cut of each snapshot, checked against the invariants, and the
// shared file mxOUT.txt as it is written.
//
// The processes are N real DIMEX modules, with a Perfect Link over TCP on
// localhost, running inside the server process. To make the flow visible,
// each CS access lasts a configurable time and message deliveries are
// delayed. The rest of the project does not depend on this command.
//
//	go run ./cmd/viz            # serves http://localhost:8080
//	go run ./cmd/viz -addr :9000
//	go run ./cmd/viz -out mxOUT.txt  # also writes the file to disk
package main

import (
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"t1sd/internal/dimex"
	"t1sd/internal/invariant"
)

//go:embed index.html
var indexHTML []byte

func main() {
	addr := flag.String("addr", "localhost:8080", "web server address")
	out := flag.String("out", "", "file that also receives the mxOUT of each run (empty: page only)")
	flag.Parse()

	s := &server{subs: make(map[chan []byte]struct{}), outPath: *out}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	mux.HandleFunc("GET /api/events", s.handleEvents)
	mux.HandleFunc("POST /api/start", s.handleStart)
	mux.HandleFunc("POST /api/stop", s.handleStop)
	mux.HandleFunc("POST /api/snapshot", s.handleSnapshot)

	log.Printf("visualization at http://%s", *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}

// params are the parameters of a run, chosen in the page.
type params struct {
	N           int    `json:"n"`
	Fault       string `json:"fault"`
	LatencyMs   int    `json:"latencyMs"`   // message delivery delay
	CSMs        int    `json:"csMs"`        // time inside the CS
	ThinkMs     int    `json:"thinkMs"`     // mean time outside the CS between accesses
	SnapEveryMs int    `json:"snapEveryMs"` // interval between automatic snapshots (0: off)
}

func (p params) validate() (dimex.Fault, error) {
	if p.N < 2 || p.N > 8 {
		return 0, errors.New("number of processes must be between 2 and 8")
	}
	if p.LatencyMs < 0 || p.CSMs < 0 || p.ThinkMs < 0 || p.SnapEveryMs < 0 {
		return 0, errors.New("times cannot be negative")
	}
	return dimex.ParseFault(p.Fault)
}

// maxHistory bounds the events kept for replay to pages that connect later.
const maxHistory = 500_000

type server struct {
	outPath string // if not empty, each run writes its mxOUT to this file

	mu      sync.Mutex
	sess    *session
	gen     int // incremented on each run; events from older runs are dropped
	subs    map[chan []byte]struct{}
	history [][]byte
}

// publish sends the event to every connected page, if gen is the current
// run.
func (s *server) publish(gen int, ev map[string]any) {
	b, err := json.Marshal(ev)
	if err != nil {
		log.Printf("event: %v", err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if gen != s.gen {
		return
	}
	if len(s.history) < maxHistory {
		s.history = append(s.history, b)
	}
	for c := range s.subs {
		select {
		case c <- b:
		default: // page is falling behind: disconnect it; it reconnects and receives the history
			delete(s.subs, c)
			close(c)
		}
	}
}

func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	c := make(chan []byte, 16384)
	s.mu.Lock()
	history := s.history
	s.subs[c] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if _, ok := s.subs[c]; ok {
			delete(s.subs, c)
			close(c)
		}
		s.mu.Unlock()
	}()

	for _, b := range history {
		fmt.Fprintf(w, "data: %s\n\n", b)
	}
	flusher.Flush()

	for {
		select {
		case b, ok := <-c:
			if !ok {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", b)
			for more := true; more; { // batch whatever has already arrived into one flush
				select {
				case b, ok := <-c:
					if !ok {
						return
					}
					fmt.Fprintf(w, "data: %s\n\n", b)
				default:
					more = false
				}
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func (s *server) handleStart(w http.ResponseWriter, r *http.Request) {
	var p params
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.start(p); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleStop(w http.ResponseWriter, r *http.Request) {
	s.stop()
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	sess := s.sess
	s.mu.Unlock()
	if sess == nil {
		http.Error(w, "no run in progress", http.StatusConflict)
		return
	}
	go sess.mods[0].Snapshot()
	w.WriteHeader(http.StatusNoContent)
}

// start stops the current run, if any, and starts a new one.
func (s *server) start(p params) error {
	fault, err := p.validate()
	if err != nil {
		return err
	}
	s.stop()

	addrs, err := freeAddrs(p.N)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.gen++
	sess := &session{
		srv:     s,
		gen:     s.gen,
		p:       p,
		start:   time.Now(),
		quit:    make(chan struct{}),
		pending: make(map[int][]*dimex.Snapshot),
	}
	s.history = nil
	s.mu.Unlock()
	if s.outPath != "" {
		f, err := os.Create(s.outPath)
		if err != nil {
			return err
		}
		sess.file = f
	}
	s.publish(sess.gen, map[string]any{"type": "session", "t": 0, "params": p})

	for i := range addrs {
		m, err := dimex.NewObserved(dimex.Config{
			ID:         i,
			Addresses:  addrs,
			Fault:      fault,
			OnSnapshot: sess.onSnapshot,
		}, time.Duration(p.LatencyMs)*time.Millisecond, sess.onLink)
		if err != nil {
			sess.close()
			return err
		}
		sess.mods = append(sess.mods, m)
	}

	for i, m := range sess.mods {
		sess.wg.Add(1)
		go sess.app(i, m)
	}
	if p.SnapEveryMs > 0 {
		sess.wg.Add(1)
		go sess.autoSnapshots()
	}

	s.mu.Lock()
	s.sess = sess
	s.mu.Unlock()
	return nil
}

func (s *server) stop() {
	s.mu.Lock()
	sess := s.sess
	s.sess = nil
	s.mu.Unlock()
	if sess != nil {
		sess.close()
		sess.publish(map[string]any{"type": "stopped", "t": sess.now()})
	}
}

// session is one run: N modules and the application of each process.
type session struct {
	srv   *server
	gen   int
	p     params
	start time.Time
	mods  []*dimex.Module
	quit  chan struct{}
	wg    sync.WaitGroup

	mu      sync.Mutex
	pending map[int][]*dimex.Snapshot // local states per snapshot, until complete

	fileMu sync.Mutex // orders the writes to the shared file
	file   *os.File   // nil: the file exists only in the page
}

func (sess *session) close() {
	close(sess.quit)
	for _, m := range sess.mods {
		m.Close() // blocked Lock and Snapshot calls return ErrClosed
	}
	sess.wg.Wait()
	if sess.file != nil {
		sess.file.Close()
	}
}

// write appends ch to the shared file, as the cmd/dimex application does
// with mxOUT.txt, and publishes the write. The lock keeps the order of the
// events in the page equal to the order in the file.
func (sess *session) write(p int, ch string) {
	sess.fileMu.Lock()
	defer sess.fileMu.Unlock()
	if sess.file != nil {
		if _, err := sess.file.WriteString(ch); err != nil {
			log.Printf("write %s: %v", sess.file.Name(), err)
		}
	}
	sess.publish(map[string]any{"type": "write", "t": sess.now(), "p": p, "ch": ch})
}

func (sess *session) now() float64 { return ms(time.Since(sess.start)) }

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func (sess *session) publish(ev map[string]any) { sess.srv.publish(sess.gen, ev) }

// app is the application of process p: it thinks, requests the CS, stays in
// it for csMs and exits. As in cmd/dimex, the first operation inside the CS
// writes "|" to the shared file and the last one writes "."; here csMs
// separates the two.
func (sess *session) app(p int, m *dimex.Module) {
	defer sess.wg.Done()
	state := func(st dimex.State) {
		sess.publish(map[string]any{"type": "state", "t": sess.now(), "p": p, "state": st.String()})
	}
	for {
		think := time.Duration(float64(sess.p.ThinkMs)*(0.25+1.5*rand.Float64())) * time.Millisecond
		if !sess.sleep(think) {
			return
		}
		state(dimex.WantMX) // before Lock, which sends the requests
		if m.Lock() != nil {
			return
		}
		state(dimex.InMX)
		sess.write(p, "|")
		if !sess.sleep(time.Duration(sess.p.CSMs) * time.Millisecond) {
			return
		}
		sess.write(p, ".")
		state(dimex.NoMX) // before Unlock, which sends the deferred replies
		if m.Unlock() != nil {
			return
		}
	}
}

func (sess *session) autoSnapshots() {
	defer sess.wg.Done()
	for sess.sleep(time.Duration(sess.p.SnapEveryMs) * time.Millisecond) {
		if _, err := sess.mods[0].Snapshot(); err != nil {
			return
		}
	}
}

// sleep waits for d; it returns false if the run is stopped first.
func (sess *session) sleep(d time.Duration) bool {
	if d <= 0 {
		select {
		case <-sess.quit:
			return false
		default:
			return true
		}
	}
	select {
	case <-time.After(d):
		return true
	case <-sess.quit:
		return false
	}
}

func (sess *session) onLink(e dimex.LinkEvent) {
	typ := "send"
	if e.Delivered {
		typ = "deliver"
	}
	sess.publish(map[string]any{
		"type": typ, "t": ms(e.Time.Sub(sess.start)),
		"from": e.From, "to": e.To,
		"kind": e.Msg.Kind, "ts": e.Msg.TS, "snap": e.Msg.Snap,
	})
}

// onSnapshot receives the complete local state of a process. Once every
// process has completed the same snapshot, it checks the invariants.
func (sess *session) onSnapshot(rec dimex.Snapshot) {
	sess.publish(map[string]any{"type": "snaplocal", "t": sess.now(), "p": rec.Process, "id": rec.ID})

	sess.mu.Lock()
	recs := append(sess.pending[rec.ID], &rec)
	sess.pending[rec.ID] = recs
	complete := len(recs) == sess.p.N
	if complete {
		delete(sess.pending, rec.ID)
	}
	sess.mu.Unlock()
	if !complete {
		return
	}

	g := invariant.Global{ID: rec.ID, Procs: make([]dimex.Snapshot, sess.p.N)}
	for _, r := range recs {
		g.Procs[r.Process] = *r
	}
	results := make([]map[string]any, len(invariant.All))
	for i, inv := range invariant.All {
		results[i] = map[string]any{"name": inv.Name, "desc": inv.Desc, "ok": inv.Check(g)}
	}
	sess.publish(map[string]any{
		"type": "snapshot", "t": sess.now(), "id": rec.ID,
		"procs": g.Procs, "invariants": results,
	})
}

func freeAddrs(n int) ([]string, error) {
	addrs := make([]string, n)
	lns := make([]net.Listener, n)
	for i := range addrs {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		lns[i] = ln
		addrs[i] = ln.Addr().String()
	}
	for _, ln := range lns {
		ln.Close()
	}
	return addrs, nil
}
