# quorum — a Raft consensus implementation in C++17

A replicated key-value store built on a real Raft implementation: leader
election, log replication, and the safety properties that make Raft
Raft (not just "leader-follower replication with extra steps") — implemented
from the paper, not a library, and communicating over real TCP sockets
between real threads, not in-process function calls.

## Why this exists

Distributed consensus is one of the few algorithms that's easy to *describe*
and notoriously easy to get *subtly wrong* — the difference between "looks
like it works" and "is actually safe under leader crashes and network
partitions" is the entire point of the Raft paper's Figure 2 and Figure 8.
This project exists to have actually hit those subtleties, not just read
about them.

## Architecture

```
        127.0.0.1:9000              127.0.0.1:9001
      ┌────────────────┐          ┌────────────────┐
      │   RaftNode 0    │          │   RaftNode 1    │   ...N nodes
      │  ┌───────────┐  │  TCP     │  ┌───────────┐  │
      │  │ Transport │◄─┼──────────┼─►│ Transport │  │
      │  └─────┬─────┘  │RequestVote│  └─────┬─────┘  │
      │        │        │AppendEnt.│        │        │
      │   state (mutex) │          │   state (mutex) │
      │   log, term,    │          │   log, term,    │
      │   commitIndex   │          │   commitIndex   │
      │        │        │          │        │        │
      │   kv_ (applied  │          │   kv_ (applied  │
      │   once committed)│         │   once committed)│
      └────────────────┘          └────────────────┘
```

- **`include/quorum/types.hpp`** — wire-level structs: `LogEntry`,
  `RequestVoteArgs/Reply`, `AppendEntriesArgs/Reply`.
- **`include/quorum/transport.hpp` / `src/transport.cpp`** — real TCP RPC.
  Each node binds a listener on `127.0.0.1:base_port+node_id`; outgoing calls
  open a short-lived connection per RPC with a 100ms socket timeout, so a
  dead or partitioned peer fails fast instead of hanging the caller. Wire
  format is a deliberately plain newline-terminated text line per RPC (see
  `EncodeRequestVoteArgs` etc. in `transport.cpp`) — the point of this
  project is Raft correctness, not payload serialization, so keys/values are
  restricted to whitespace/colon-free tokens.
- **`include/quorum/raft_node.hpp` / `src/raft_node.cpp`** — the state
  machine: election timeouts, `RequestVote`/`AppendEntries` handling, log
  matching, commit-index advancement, and the KV layer applied on commit.
- **`include/quorum/partition_filter.hpp`** — test-only fault injection
  sitting *above* the real transport: every RPC still opens a real socket and
  serializes a real message, this just decides whether an attempt is allowed
  through, so tests can simulate a network partition without touching OS
  routing.
- **`src/main.cpp`** — a demo CLI that boots an N-node cluster and lets you
  `put`/`get`/`kill`/`revive` nodes interactively.

## Safety properties implemented, and how each is tested

| Property (Raft paper) | Where enforced | Test |
|---|---|---|
| **Election Safety** — at most one leader per term | `RequestVote`'s term/log-comparison rules | `five_node_cluster_elects_exactly_one_leader` + a persistent `ElectionLog` (see below) checked in every test via `AssertElectionSafety()` |
| **Leader Append-Only / Log Matching** | `AppendEntries` consistency check (`prev_log_index`/`prev_log_term`) + conflicting-suffix truncation | `basic_replication_put_then_get_from_majority`, `concurrent_load_...` (compares full log prefixes across nodes at the end) |
| **Leader Completeness / Figure 8 safety** — a leader only commits entries from its *own* current term directly; earlier-term entries are committed only as a side effect | `TryAdvanceCommitIndexLocked`'s `TermAtLocked(n) != current_term_` guard, plus a no-op entry appended on election so a new leader always has *something* from its own term to commit | see "Hardest bug" below — this is the one that actually broke |
| **No progress without a majority** | commit only advances when `count > cluster_size_/2` | `minority_partition_makes_no_progress_majority_does` |
| **Stale leader steps down** | any RPC/reply carrying a higher term reverts the node to Follower | same test — the isolated leader is confirmed to give up leadership once the partition heals |

`ElectionLog` deserves a callout: polling `GetRole()` periodically can miss a
node that won and then lost leadership between two polls. Every real
leadership transition is instead recorded as it happens (a pattern real Raft
libraries expose too — e.g. `hashicorp/raft`'s `leaderCh`), and tests check
the *entire* recorded history, not a snapshot.

## Hardest bug

Two, actually — one algorithmic, one concurrency. Both are the reason this
project has the test suite it has rather than a smaller one.

**1. Figure 8: committing across a term boundary.** The first working version
advanced `commitIndex` whenever an entry was replicated on a majority,
full stop. That's wrong, and it's wrong in a way that only shows up under a
specific leader-crash sequence: a leader can replicate an entry to a majority
and then crash *before* committing it; a new leader (different term) can
then append and commit its *own* entry at that same index without ever
having seen the old one, which is safe — but only if the old leader's
uncommitted-but-majority-replicated entry is never unilaterally committed by
a future leader just because a majority happens to still have it. The fix is
the guard in `TryAdvanceCommitIndexLocked`: a leader only *directly* commits
entries from its own current term; entries from earlier terms become
committed only as a side effect of a later same-term entry committing over
them. This is also why `BecomeLeaderLocked` appends a no-op entry immediately
on election — without it, a new leader with no writes of its own could sit
forever unable to safely advance `commitIndex` even once caught up.

**2. A real data race, caught by ThreadSanitizer, not by eye.** `Transport`
closes its listening socket from the owning thread (`Stop()`) while the
listener thread is blocked in `accept()` on that same file descriptor.
`listen_fd_` was a plain `int` — TSan flagged it as a genuine data race
(write in `Stop()`, concurrent read in `ListenLoop()`'s `accept()` call, no
synchronization between them), and it was real: nothing here is a "logical
race that's fine in practice," it's undefined behavior under the C++ memory
model regardless of how consistently it happened to work in manual testing.
Every test passed cleanly before this fix and every test still passes
cleanly after it — TSan is what caught it, not the test suite's assertions,
which is exactly why `make tsan` is a first-class target here and not an
afterthought. Fixed by making `listen_fd_` a `std::atomic<int>`.

## Deliberate limitations

- **No persistence across restart.** Real Raft requires `currentTerm`,
  `votedFor`, and the log to survive a crash — this implementation is
  in-memory only. `Stop()`/`Start()` here simulate a node going
  unresponsive (paused, or network-isolated) and later reachable again,
  **not** a true process crash that loses volatile state and rejoins fresh.
  Persisting to disk (and handling a node that rejoins with *no* prior
  state, which needs a snapshot-transfer path) is the natural next step.
- **Reads are not linearizable.** `Get()` serves from the local applied
  state of whichever node the caller believes is the leader. A real
  deployment needs the read-index or lease-read protocol from the extended
  Raft paper to guarantee a read reflects the latest committed write — this
  implementation doesn't have either, so a stale leader that hasn't yet
  learned it's been deposed could serve a stale read for up to one election
  timeout.
- **No log compaction / snapshotting.** The log grows unboundedly; a
  long-running cluster would need periodic snapshots (Raft paper §7).
- **Thread-per-RPC, not an event loop.** Elections and heartbeat broadcasts
  spawn one thread per peer and join before returning, bounded by the 100ms
  RPC timeout. Simple to reason about and correct at cluster sizes this
  project targets (a handful of nodes); a production system at larger scale
  would want a proper async I/O reactor instead of a thread per outstanding
  RPC.

## Building and running

```bash
make demo   # build build/quorum_demo
make test   # build + run the correctness suite
make tsan   # build + run the suite under ThreadSanitizer
```

Demo session:

```
$ ./build/quorum_demo 5
quorum: 5-node Raft cluster started on 127.0.0.1:9000-9004
waiting for initial leader election...
> status
  node 4 [term 1] leader commitIndex=1 logLen=1
> put foo bar
  OK (committed via node 4)
> get foo
  bar
> kill 4
  node 4 stopped
> status
  node 0 [term 2] leader commitIndex=2 logLen=2   # new leader elected automatically
> get foo
  bar                                              # survived the leader change
```

## Testing

```bash
make test
```

```
RUN  five_node_cluster_elects_exactly_one_leader
PASS five_node_cluster_elects_exactly_one_leader
RUN  basic_replication_put_then_get_from_majority
PASS basic_replication_put_then_get_from_majority
RUN  non_leader_put_is_rejected_immediately
PASS non_leader_put_is_rejected_immediately
RUN  killing_the_leader_elects_a_new_one_and_cluster_keeps_committing
PASS killing_the_leader_elects_a_new_one_and_cluster_keeps_committing
RUN  minority_partition_makes_no_progress_majority_does
PASS minority_partition_makes_no_progress_majority_does
RUN  concurrent_load_with_follower_churn_never_loses_a_committed_entry
PASS concurrent_load_with_follower_churn_never_loses_a_committed_entry

6 passed, 0 failed (6 total)
```

Every test talks to real `RaftNode` instances over real TCP on
`127.0.0.1` — nothing here is mocked. `concurrent_load_with_follower_churn`
runs 4 concurrent writer threads issuing 60 total `Put`s while a follower is
stopped and restarted mid-run, then walks every surviving node's full log and
asserts byte-for-byte agreement with the leader up to the lowest commit index
any node reached — not just "did it not crash."
