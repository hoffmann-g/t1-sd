package invariant

import (
	"fmt"
	"io"
)

// Print evaluates all invariants on all complete snapshots and writes the
// report to w: each violation with the snapshot where it occurred, the state
// of up to details violated snapshots per invariant, and the summary.
// It returns true if no invariant was violated.
func (r *Report) Print(w io.Writer, details int) bool {
	violations := make(map[string][]int) // invariant → snapshot ids
	shown := make(map[string]int)
	for _, g := range r.Globals {
		for _, inv := range g.Violated() {
			violations[inv.Name] = append(violations[inv.Name], g.ID)
			fmt.Fprintf(w, "VIOLATION snapshot %d: %s (%s)\n", g.ID, inv.Name, inv.Desc)
			if shown[inv.Name] < details {
				shown[inv.Name]++
				fmt.Fprint(w, g)
			}
		}
	}

	fmt.Fprintf(w, "\n%d processes, %d complete snapshots", r.N, len(r.Globals))
	if len(r.Incomplete) > 0 {
		fmt.Fprintf(w, ", %d incomplete ignored %v", len(r.Incomplete), r.Incomplete)
	}
	if r.BadLines > 0 {
		fmt.Fprintf(w, ", %d unreadable lines ignored", r.BadLines)
	}
	fmt.Fprintln(w)

	ok := true
	for _, inv := range All {
		ids := violations[inv.Name]
		status := "ok"
		if len(ids) > 0 {
			ok = false
			status = fmt.Sprintf("VIOLATED in %d snapshots", len(ids))
		}
		fmt.Fprintf(w, "  %-5s %-26s %s\n", inv.Name, status, inv.Desc)
	}
	return ok
}
