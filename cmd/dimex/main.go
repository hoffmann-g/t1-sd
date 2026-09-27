// Command dimex runs one process of the DIMEX test application.
//
// The process requests the CS repeatedly, without pauses, and inside it
// writes "|" and then "." to the shared file. With correct mutual exclusion
// the file contains only "|.|.|.|."; any "||" or ".." is a violation.
//
// The process with -snapshots > 0 is the initiator: it starts that many
// successive snapshots, concurrently with its own CS accesses. Every process
// records its local state for each snapshot in <snapdir>/p<id>.jsonl.
//
// Example with 3 processes, one per terminal, from the project root:
//
//	go run ./cmd/dimex -id 0 -addrs 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002 -snapshots 300
//	go run ./cmd/dimex -id 1 -addrs 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002
//	go run ./cmd/dimex -id 2 -addrs 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002
//
// After finishing its rounds and snapshots, the process prints "DONE" and
// stays active, replying to the others, until it receives Ctrl+C.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"t1sd/internal/dimex"
)

func main() {
	id := flag.Int("id", -1, "index of this process in -addrs")
	addrs := flag.String("addrs", "", "addresses of all processes, comma-separated")
	rounds := flag.Int("rounds", 0, "CS accesses before stopping (0 = no limit)")
	out := flag.String("out", "mxOUT.txt", "shared CS file")
	snapshots := flag.Int("snapshots", 0, "snapshots to start from this process")
	interval := flag.Duration("snapinterval", time.Millisecond, "pause between successive snapshots")
	snapdir := flag.String("snapdir", "snapshots", "directory for the snapshot files")
	faultName := flag.String("fault", "none", "injected fault: none, mutex or block")
	verbose := flag.Bool("v", false, "log link and module events to stderr")
	flag.Parse()

	log.SetFlags(log.Lmicroseconds)
	log.SetPrefix(fmt.Sprintf("[p%d] ", *id))

	fault, err := dimex.ParseFault(*faultName)
	if err != nil {
		log.Fatal(err)
	}
	if *addrs == "" {
		flag.Usage()
		os.Exit(2)
	}

	snapFile, err := createSnapshotFile(*snapdir, *id)
	if err != nil {
		log.Fatal(err)
	}
	defer snapFile.Close()
	enc := json.NewEncoder(snapFile)

	var logger *log.Logger
	if *verbose {
		logger = log.Default()
	}
	mod, err := dimex.New(dimex.Config{
		ID:        *id,
		Addresses: strings.Split(*addrs, ","),
		Fault:     fault,
		OnSnapshot: func(s dimex.Snapshot) {
			// One write per snapshot, directly to the file: if the process is
			// killed, at most the last line is left incomplete.
			if err := enc.Encode(s); err != nil {
				log.Printf("record snapshot %d: %v", s.ID, err)
			}
		},
		Logger: logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer mod.Close()

	file, err := os.OpenFile(*out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var wg sync.WaitGroup
	if *snapshots > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			takeSnapshots(mod, *snapshots, *interval)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := useCriticalSection(mod, file, *rounds); err != nil {
			log.Fatal(err)
		}
	}()

	stop, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
		fmt.Println("DONE")
		<-stop.Done() // keep replying to the other processes
	case <-stop.Done():
	}
	log.Print("shutting down")
}

// useCriticalSection performs rounds CS accesses (rounds = 0: no limit).
func useCriticalSection(mod *dimex.Module, file *os.File, rounds int) error {
	for i := 0; rounds == 0 || i < rounds; i++ {
		if err := mod.Lock(); err != nil {
			return err
		}
		if _, err := file.WriteString("|"); err != nil {
			return err
		}
		if _, err := file.WriteString("."); err != nil {
			return err
		}
		if err := mod.Unlock(); err != nil {
			return err
		}
	}
	log.Printf("%d CS accesses completed", rounds)
	return nil
}

func takeSnapshots(mod *dimex.Module, n int, interval time.Duration) {
	for i := 0; i < n; i++ {
		if _, err := mod.Snapshot(); err != nil {
			log.Printf("snapshot: %v", err)
			return
		}
		time.Sleep(interval)
	}
	log.Printf("%d snapshots completed", n)
}

func createSnapshotFile(dir string, id int) (*os.File, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return os.Create(filepath.Join(dir, fmt.Sprintf("p%d.jsonl", id)))
}
