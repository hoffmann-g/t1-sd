// Command checker evaluates the snapshots recorded by the dimex processes.
//
// For each complete snapshot (with the state of every process), it evaluates
// all invariants of package invariant and reports each violation with the
// snapshot where it occurred. It exits with code 1 if any violation is found.
//
//	go run ./cmd/checker -dir snapshots
package main

import (
	"flag"
	"log"
	"os"

	"t1sd/internal/invariant"
)

func main() {
	dir := flag.String("dir", "snapshots", "directory with the p<id>.jsonl files")
	details := flag.Int("details", 3, "how many violated snapshots to show per invariant")
	flag.Parse()
	log.SetFlags(0)

	rep, err := invariant.Load(*dir)
	if err != nil {
		log.Fatal(err)
	}
	if !rep.Print(os.Stdout, *details) {
		os.Exit(1)
	}
}
