// Command runner runs the complete test on a single machine:
//
//  1. builds cmd/dimex;
//  2. starts N processes on free localhost ports, with p0 as the snapshot
//     initiator;
//  3. waits for all of them to print DONE (or for the timeout, which
//     indicates deadlock);
//  4. checks the mxOUT.txt file ("||" or ".." indicate a mutual exclusion
//     violation);
//  5. evaluates the recorded snapshots against the invariants.
//
// It must be run from the project root:
//
//	go run ./cmd/runner                 # correct algorithm
//	go run ./cmd/runner -fault mutex    # fault that violates mutual exclusion
//	go run ./cmd/runner -fault block    # fault that deadlocks the processes
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"t1sd/internal/invariant"
)

func main() {
	n := flag.Int("n", 3, "number of processes")
	rounds := flag.Int("rounds", 3000, "CS accesses per process")
	snapshots := flag.Int("snapshots", 300, "snapshots started by p0")
	interval := flag.Duration("snapinterval", time.Millisecond, "pause between successive snapshots")
	fault := flag.String("fault", "none", "injected fault: none, mutex or block")
	timeout := flag.Duration("timeout", 30*time.Second, "deadline for all processes to finish")
	dir := flag.String("dir", "run", "output directory (deleted at startup)")
	details := flag.Int("details", 2, "how many violated snapshots to show per invariant")
	flag.Parse()
	log.SetFlags(0)

	if err := os.RemoveAll(*dir); err != nil {
		log.Fatal(err)
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		log.Fatal(err)
	}
	bin, err := build(*dir)
	if err != nil {
		log.Fatal(err)
	}
	addrs, err := freeAddrs(*n)
	if err != nil {
		log.Fatal(err)
	}

	mxPath := filepath.Join(*dir, "mxOUT.txt")
	snapDir := filepath.Join(*dir, "snapshots")
	fmt.Printf("%d processes, %d accesses each, %d snapshots, fault=%s\n", *n, *rounds, *snapshots, *fault)

	start := time.Now()
	procs := make([]*exec.Cmd, *n)
	done := make(chan int, *n)
	for i := range procs {
		args := []string{
			"-id", strconv.Itoa(i),
			"-addrs", strings.Join(addrs, ","),
			"-rounds", strconv.Itoa(*rounds),
			"-out", mxPath,
			"-snapdir", snapDir,
			"-fault", *fault,
		}
		if i == 0 {
			args = append(args, "-snapshots", strconv.Itoa(*snapshots), "-snapinterval", interval.String())
		}
		cmd := exec.Command(bin, args...)
		cmd.Stderr = os.Stderr
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			log.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			log.Fatal(err)
		}
		procs[i] = cmd
		go watchDone(i, stdout, done)
	}

	finished := waitAll(done, *n, *timeout)
	elapsed := time.Since(start)
	for _, cmd := range procs {
		cmd.Process.Kill()
		cmd.Wait()
	}

	ok := true
	fmt.Println("\n== Mutual exclusion (DMX1 and DMX2) ==")
	if finished {
		fmt.Printf("all processes finished in %v\n", elapsed.Round(time.Millisecond))
	} else {
		ok = false
		fmt.Printf("DMX1 VIOLATED: processes did not finish within %v (deadlock)\n", *timeout)
	}
	if !checkMX(mxPath, *n**rounds, finished) {
		ok = false
	}

	fmt.Println("\n== Snapshots ==")
	rep, err := invariant.Load(snapDir)
	if err != nil {
		log.Fatal(err)
	}
	if !rep.Print(os.Stdout, *details) {
		ok = false
	}

	if !ok {
		fmt.Println("\nRESULT: violations detected")
		os.Exit(1)
	}
	fmt.Println("\nRESULT: no violations")
}

// build compiles cmd/dimex into dir and returns the path of the executable.
func build(dir string) (string, error) {
	name := "dimex"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	bin, err := filepath.Abs(filepath.Join(dir, name))
	if err != nil {
		return "", err
	}
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/dimex")
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("build cmd/dimex (run from the project root): %v\n%s", err, out)
	}
	return bin, nil
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

// watchDone reads the output of process id and signals on done when it prints DONE.
func watchDone(id int, r io.Reader, done chan<- int) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		if sc.Text() == "DONE" {
			done <- id
		}
	}
}

func waitAll(done <-chan int, n int, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for i := 0; i < n; i++ {
		select {
		case <-done:
		case <-deadline:
			return false
		}
	}
	// Gives the other processes time to record the last snapshot, which the
	// initiator completes before them.
	time.Sleep(200 * time.Millisecond)
	return true
}

// checkMX checks the CS file. If all processes finished, the file must
// contain exactly accesses "|." pairs.
func checkMX(path string, accesses int, finished bool) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("could not read %s: %v\n", path, err)
		return false
	}
	s := string(data)
	bars, dots := strings.Count(s, "||"), strings.Count(s, "..")
	fmt.Printf("%s: %d bytes, %d occurrences of \"||\", %d of \"..\"\n", path, len(s), bars, dots)

	ok := true
	if bars > 0 || dots > 0 {
		ok = false
		fmt.Println("DMX2 VIOLATED: simultaneous CS accesses")
	}
	if finished && s != strings.Repeat("|.", accesses) {
		ok = false
		fmt.Printf("content differs from %d \"|.\" pairs\n", accesses)
	}
	return ok
}
