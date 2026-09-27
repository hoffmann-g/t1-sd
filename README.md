# Distributed mutual exclusion with Chandy-Lamport snapshots

A Go implementation of distributed mutual exclusion (Ricart-Agrawala with Lamport clocks) extended with the Chandy-Lamport snapshot algorithm, running on a Perfect Link built over TCP. A separate tool reads the recorded snapshots and checks nine system invariants on each one. Two injected faults, one that breaks mutual exclusion and one that deadlocks the processes, show that the snapshot analysis detects both kinds of violation.

Assignment 1 of the Distributed Systems course (4647B-04, PUCRS, 2026/2).

Authors: George Rother, Guilherme Hoffmann, João Sbardelotto.

## Overview

N processes compete for a shared file. Each process repeatedly asks for the critical section (CS), writes `|` and then `.` to the file, and releases the CS, with no pauses between accesses. If mutual exclusion holds, the file contains only `|.|.|.|.`. Any `||` or `..` means two processes were in the CS at the same time.

While the processes run, process 0 starts hundreds of snapshots. Each snapshot is a consistent global state: the variables of every process plus the messages in transit on every channel, recorded without stopping the system. A checker loads each snapshot and evaluates invariants that must hold in every reachable state of a correct system. A violated invariant points to the exact snapshot where the system reached an invalid state.

The project uses only the Go standard library.

## Architecture

```
┌──────────────────────────────────────────────┐
│ Application (cmd/dimex)                      │  Lock / Unlock / Snapshot
├──────────────────────────────────────────────┤
│ DIMEX module (internal/dimex)                │  Ricart-Agrawala + Chandy-Lamport
│   one goroutine owns all state               │
├──────────────────────────────────────────────┤
│ Perfect Link (internal/pl)                   │  Send / Ind
│   one TCP connection per (sender, receiver)  │
└──────────────────────────────────────────────┘
```

Each layer uses only the layer directly below it. The application calls `Lock`, `Unlock` and `Snapshot` on the DIMEX module. The DIMEX module calls `Send` and reads deliveries from `Ind` on the Perfect Link. The DIMEX module does not open sockets, and the application does not see messages.

```
internal/pl         Perfect Link over TCP
internal/dimex      mutual exclusion (Ricart-Agrawala) and snapshot (Chandy-Lamport)
internal/invariant  invariants, snapshot loading and report
cmd/dimex           test application: one process
cmd/checker         snapshot analysis tool
cmd/runner          full test on one machine: starts N processes and checks the results
cmd/viz             web visualization of an execution (optional)
```

## Perfect Link (`internal/pl`)

The Perfect Link guarantees three properties for processes that do not crash:

- PL1 (reliable delivery): if p sends m to q, q delivers m.
- PL2 (no duplication): no message is delivered more than once.
- PL3 (no creation): if q delivers m with sender p, then p sent m.

Each (sender, receiver) pair uses a single TCP connection, opened by the sender. TCP provides delivery, ordering and no duplication while the connection is open. Since there is only one connection per pair, messages from one sender arrive in the order they were sent (FIFO). Chandy-Lamport requires FIFO channels, so this property is what makes the snapshot algorithm correct on top of this link.

Framing: each message is sent as a 4-byte length followed by the payload. The first message on each connection carries the listening address of the process that opened it. That address is the sender reported with every delivery, and the DIMEX module maps it to a process id.

`Send` does not wait for the network. The message goes into an unbounded queue for its destination, and one goroutine per destination drains that queue into the connection. Without this, two DIMEX event loops that send to each other at the same time could both block on a full TCP buffer and never read their incoming messages. If the destination is not listening yet, the sender retries the connection until it succeeds, so processes can start in any order.

## Mutual exclusion (`internal/dimex`)

The module implements Ricart-Agrawala with a Lamport logical clock. Each process is in one of three states: `noMX` (does not want the CS), `wantMX` (has requested the CS and waits for replies) and `inMX` (is in the CS).

- `Lock`: increments the clock, sends `req(ts)` to the other N-1 processes and moves to `wantMX`. The process enters the CS (`inMX`) after receiving N-1 `ok` replies.
- On receiving `req(ts)` from q:
  - in `noMX`, reply `ok` immediately;
  - in `inMX`, defer the reply by setting `waiting[q]`;
  - in `wantMX`, defer if the own request has priority, that is, if `(reqTs, id) < (ts, q)`; otherwise reply. Ties on the timestamp are broken by the smaller id, so two requests are never equal.
- `Unlock`: sends `ok` to every deferred process and returns to `noMX`.

Properties:

- DMX1 (no postponement and no deadlock): every call to `Lock` returns.
- DMX2 (mutual exclusion): if `Lock` returned in p, `Lock` does not return in any other process before p calls `Unlock`.

DMX2 holds because two processes in `wantMX` compare the same pair of `(timestamp, id)` values, and exactly one of them defers the other. DMX1 holds because the `(timestamp, id)` order is total, so the waiting relation has no cycles, and the process with the smallest pending request always receives all replies.

### Concurrency model

All state of both algorithms belongs to a single goroutine, the event loop. It handles one event at a time: either a request from the application (`Lock`, `Unlock`, `Snapshot`) or a message delivered by the link. Each event is atomic, as in the event-based specification presented in class, and the state needs no mutexes. `Lock` sends a request into the loop and blocks until the loop closes a channel that signals the CS was granted.

### Snapshot

The same module implements Chandy-Lamport, with a `marker(id)` message:

- The initiator records its local state, starts recording every incoming channel and sends `marker(id)` to every other process.
- On the first `marker(id)`, a process records its local state, marks the channel the marker came from as empty, starts recording the other incoming channels and sends `marker(id)` to every other process.
- On each later `marker(id)`, the process stops recording the channel it came from.
- A `req` or `ok` message that arrives on a channel being recorded is appended to that channel's state before it is handled.
- When every incoming channel has received the marker, the local state is complete. It is written to `snapshots/p<id>.jsonl`, one JSON line per snapshot.

Each record contains the snapshot id, the process id, the algorithm variables (`state`, `clock`, `reqTs`, `resps`, `waiting`) and, for each incoming channel, the messages in transit on it. Two records from a correct run with 3 processes:

```json
{"id":1,"process":1,"state":"wantMX","clock":2,"reqTs":1,"resps":0,"waiting":[false,false,false],"channels":{"0":[],"2":[{"kind":"req","ts":1},{"kind":"ok"}]}}
{"id":6,"process":2,"state":"inMX","clock":156,"reqTs":151,"resps":2,"waiting":[true,true,false],"channels":{"0":[],"1":[]}}
```

In snapshot 1, process 1 has requested the CS and has no replies yet. The channel from process 2 holds a request from 2 and the reply from 2 to process 1's request: both requests have timestamp 1, and process 1 wins the tie by id. In snapshot 6, process 2 is in the CS and has deferred the requests of processes 0 and 1.

Several snapshots can be in progress at the same time. The initiator starts the next snapshot as soon as its own local state for the previous one is complete, while other processes may still be recording channels for the previous one. Ids are 1, 2, 3..., which assumes a single initiator.

### Injected faults

The `-fault` flag changes the rule that decides whether to reply to or defer a request received in `wantMX` (function `shouldDefer` in [internal/dimex/dimex.go](internal/dimex/dimex.go)):

- `mutex`: always reply, without comparing priorities. Two processes that request at the same time enter the CS together. Violates DMX2.
- `block`: always defer, without comparing priorities. Two processes that request at the same time wait for each other forever. Violates DMX1.

## Invariants (`internal/invariant`)

Each invariant is a function `InvX(Global) bool`. A `Global` gathers the local state of every process for the same snapshot id. Snapshots missing the state of some process (for example, because the run was interrupted) are skipped and counted in the report.

| Invariant | Statement |
|---|---|
| Inv1 | At most one process is in `inMX`. |
| Inv2 | If every process is in `noMX`, no `waiting` flag is set and no channel has messages. |
| Inv3 | If p has set `waiting[q]`, p is in `wantMX` or `inMX`. |
| Inv4 | If p has set `waiting[q]`, q is in `wantMX`. |
| Inv5 | If q is in `wantMX`: `resps(q)` + `ok` in transit to q + `waiting[q]` set in other processes + `req` from q in transit = N-1. |
| Inv6 | There are no p and q such that p has `waiting[q]` set and q has `waiting[p]` set (circular wait). |
| Inv7 | `resps` is 0 in `noMX`, less than N-1 in `wantMX` and equal to N-1 in `inMX`. |
| Inv8 | If p is in `wantMX` and has set `waiting[q]`, then `(reqTs(p), p) < (reqTs(q), q)`. |
| Inv9 | If q is not in `wantMX`, there is no `req` from q and no `ok` to q in transit. |

Why the less obvious ones hold in a correct execution:

- Inv5: while q waits, each of the other N-1 processes is in exactly one of four situations. Either q's request is still in transit to it, or it deferred q, or its reply is in transit to q, or q has already received that reply. Counting the four cases gives N-1.
- Inv6 and Inv8: a process in `wantMX` only defers a request with lower priority than its own. Two processes cannot each have lower priority than the other, so a circular wait between two processes in `wantMX` is impossible. If p is in `inMX`, every other process has already replied to p, so none of them is deferring p.
- Inv9: `req` messages are only sent on entering `wantMX`, and `ok` messages are only sent to processes that requested. Once q leaves `wantMX`, it has received all N-1 replies, and every request it sent has been delivered.

Inv1 is the snapshot-level view of DMX2. Inv6 and Inv8 detect the `block` fault in a recorded state, before the deadlock is observed as a timeout.

### Expected results

| Run | mxOUT.txt | Violated invariants |
|---|---|---|
| correct | only `\|.` | none |
| `-fault mutex` | contains `\|\|` and `..` | Inv1 |
| `-fault block` | few accesses; processes do not finish (DMX1) | Inv6 and Inv8 |

Output of `go run ./cmd/runner` (3 processes, 3000 accesses each, 300 snapshots):

```
== Mutual exclusion (DMX1 and DMX2) ==
all processes finished in 1.1s
run/mxOUT.txt: 18000 bytes, 0 occurrences of "||", 0 of ".."

== Snapshots ==

3 processes, 300 complete snapshots
  Inv1  ok                         at most one process in the CS
  Inv2  ok                         if no process wants the CS, there is no waiting and no message in transit
  ...
  Inv9  ok                         a process that does not want the CS has no request or reply in transit

RESULT: no violations
```

With `-fault mutex`, the shared file has 929 occurrences of `||` and the checker reports each snapshot that caught two processes in the CS:

```
VIOLATION snapshot 12: Inv1 (at most one process in the CS)
snapshot 12
  p0: noMX   reqTs=344   resps=0 waiting=[]
  p1: inMX   reqTs=346   resps=2 waiting=[]
  p2: inMX   reqTs=347   resps=2 waiting=[]
...
  Inv1  VIOLATED in 25 snapshots   at most one process in the CS
```

With `-fault block`, all three processes request the CS with timestamp 1, each one defers the other two, and none of them enters the CS. The runner stops at the timeout, and the snapshots show the circular wait:

```
snapshot 2
  p0: wantMX reqTs=1     resps=0 waiting=[1 2]
  p1: wantMX reqTs=1     resps=0 waiting=[0 2]
  p2: wantMX reqTs=1     resps=0 waiting=[0 1]
...
  Inv6  VIOLATED in 299 snapshots  no two processes defer each other
  Inv8  VIOLATED in 300 snapshots  p in wantMX defers q only if the request of p has priority
```

## Running

Go 1.22 or later. All commands run from the repository root.

### Full test

```sh
go run ./cmd/runner
```

The runner builds `cmd/dimex`, starts 3 processes on free localhost ports and waits for each one to complete 3000 CS accesses. Meanwhile process 0 starts 300 snapshots. At the end the runner checks `run/mxOUT.txt` and evaluates the snapshots in `run/snapshots/` against the invariants. The exit code is 1 if any violation is found.

With the injected faults:

```sh
go run ./cmd/runner -fault mutex             # violates mutual exclusion
go run ./cmd/runner -fault block -timeout 8s # deadlocks the processes
```

Main flags: `-n` (processes), `-rounds` (accesses per process), `-snapshots`, `-fault`, `-timeout`, `-dir` (output directory). `go run ./cmd/runner -h` lists all of them.

### Processes in separate terminals

One terminal per process. All processes receive the same address list, and each one listens on the address at the position of its id.

```sh
go run ./cmd/dimex -id 0 -addrs 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002 -rounds 5000 -snapshots 300
go run ./cmd/dimex -id 1 -addrs 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002 -rounds 5000
go run ./cmd/dimex -id 2 -addrs 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002 -rounds 5000
```

Processes can start in any order, because the Perfect Link retries the connection until the destination accepts it. When a process finishes, it prints `DONE` and keeps replying to the others until it receives Ctrl+C. The `mxOUT.txt` file and the `snapshots/` directory are created in the current directory. Passing `-fault mutex` or `-fault block` to every process enables the corresponding fault.

To evaluate the snapshots afterwards:

```sh
go run ./cmd/checker -dir snapshots
```

### Visualization (optional)

```sh
go run ./cmd/viz                  # http://localhost:8080
go run ./cmd/viz -out mxOUT.txt   # also writes the shared file to disk
```

The page shows one timeline per process, with its states (`wantMX`, `inMX`), the messages (`req`, `ok`, `marker`) and the cut of each snapshot. A cut turns red when the snapshot violates an invariant. Clicking a cut shows the recorded state of each process and the result of each invariant. Intervals with two processes in the CS are highlighted. Below the timeline, a tape shows the shared file as it is written, with the author process of each character and every `||` and `..` in red.

The page controls the number of processes, the fault, the message latency, the time spent inside and outside the CS, and the interval between snapshots. When the fault changes, the page suggests parameters that make its effect visible within a few seconds. The timeline zooms with the buttons above it, the mouse wheel, double click and the `+`, `-` and `0` keys.

The processes in the page are real DIMEX modules, with the Perfect Link over localhost TCP, running inside the server. Message deliveries are delayed so that the message flow is visible. The rest of the project does not depend on this command. It only uses [internal/dimex/observe.go](internal/dimex/observe.go), which builds a module whose link is wrapped by a transport that reports sends and deliveries.

### Automated tests

```sh
go test ./...
go test -race ./...
```

- `internal/pl`: delivery with the sender address, sending before the destination exists, FIFO order without loss or duplication over 5000 messages, several senders, binary payloads up to 1 MiB, rejection of messages above the size limit, port already in use, and `Close`.
- `internal/dimex`, unit tests: each event handler in isolation, with an in-memory transport. They cover the `(ts, id)` priority, the Lamport clock, replying and deferring in each state, both faults, and the recording of local state and channel state in snapshots, including overlapping snapshots.
- `internal/dimex`, system tests: 2, 3 and 5 real processes on localhost. They check DMX1 and DMX2 through the shared file and through a counter of processes inside the CS, that `Lock` blocks while another process is in the CS, and that no process starves. They also run 500 snapshots with the correct algorithm and check that no invariant is violated, and that the snapshots detect both faults.
- `internal/invariant`: valid states that must not report a violation, one state that violates each invariant, and the assembly and loading of snapshot files.
