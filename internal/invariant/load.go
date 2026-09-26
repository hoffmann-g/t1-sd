package invariant

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"t1sd/internal/dimex"
)

// Report is the result of reading the snapshot files.
type Report struct {
	N          int      // number of processes
	Globals    []Global // complete snapshots, in id order
	Incomplete []int    // ids missing the state of some process
	BadLines   int      // unreadable lines (process terminated mid-write)
}

// Load reads the *.jsonl files in dir (one per process, one local snapshot
// per line) and assembles the global snapshots.
func Load(dir string) (*Report, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no .jsonl files in %s", dir)
	}
	var recs []dimex.Snapshot
	bad := 0
	for _, f := range files {
		r, b, err := readFile(f)
		if err != nil {
			return nil, err
		}
		recs = append(recs, r...)
		bad += b
	}
	rep, err := Assemble(recs)
	if err != nil {
		return nil, err
	}
	rep.BadLines = bad
	return rep, nil
}

// readFile reads a snapshot file. A line that fails to decode is counted
// and skipped: this happens on the last line when the process is killed
// during a write.
func readFile(path string) ([]dimex.Snapshot, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	var recs []dimex.Snapshot
	bad := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for sc.Scan() {
		var s dimex.Snapshot
		if err := json.Unmarshal(sc.Bytes(), &s); err != nil {
			bad++
			continue
		}
		recs = append(recs, s)
	}
	if err := sc.Err(); err != nil {
		return nil, 0, fmt.Errorf("%s: %w", path, err)
	}
	return recs, bad, nil
}

// Assemble groups local states by snapshot id. A snapshot is complete when
// there is exactly one state from each process 0..N-1, where N is the length
// of the waiting vector.
func Assemble(recs []dimex.Snapshot) (*Report, error) {
	rep := &Report{}
	if len(recs) == 0 {
		return rep, nil
	}
	n := len(recs[0].Waiting)
	rep.N = n

	byID := map[int][]*dimex.Snapshot{}
	for i := range recs {
		s := &recs[i]
		if len(s.Waiting) != n {
			return nil, fmt.Errorf("snapshot %d, p%d: waiting has %d entries, want %d", s.ID, s.Process, len(s.Waiting), n)
		}
		if s.Process < 0 || s.Process >= n {
			return nil, fmt.Errorf("snapshot %d: process %d out of range [0, %d)", s.ID, s.Process, n)
		}
		procs := byID[s.ID]
		if procs == nil {
			procs = make([]*dimex.Snapshot, n)
			byID[s.ID] = procs
		}
		if procs[s.Process] != nil {
			return nil, fmt.Errorf("snapshot %d: p%d recorded twice", s.ID, s.Process)
		}
		procs[s.Process] = s
	}

	ids := make([]int, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	for _, id := range ids {
		g := Global{ID: id, Procs: make([]dimex.Snapshot, n)}
		complete := true
		for p, s := range byID[id] {
			if s == nil {
				complete = false
				break
			}
			g.Procs[p] = *s
		}
		if complete {
			rep.Globals = append(rep.Globals, g)
		} else {
			rep.Incomplete = append(rep.Incomplete, id)
		}
	}
	return rep, nil
}
