#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace quorum {

// A single replicated log entry. `key`/`value` are restricted to
// whitespace/colon-free tokens (see README) so the wire format can stay a
// simple colon-delimited text line instead of pulling in a real
// serialization library — the interesting part of this project is Raft
// itself, not payload encoding.
struct LogEntry {
    int64_t term = 0;
    std::string key;
    std::string value;  // empty for a no-op entry committed on leader election
    bool is_noop = false;
};

enum class Role { Follower, Candidate, Leader };

struct RequestVoteArgs {
    int64_t term = 0;
    int candidate_id = 0;
    int64_t last_log_index = 0;
    int64_t last_log_term = 0;
};

struct RequestVoteReply {
    int64_t term = 0;
    bool vote_granted = false;
    bool ok = false;  // false if the RPC itself failed (timeout/refused/partitioned)
};

struct AppendEntriesArgs {
    int64_t term = 0;
    int leader_id = 0;
    int64_t prev_log_index = 0;
    int64_t prev_log_term = 0;
    std::vector<LogEntry> entries;  // empty => heartbeat
    int64_t leader_commit = 0;
};

struct AppendEntriesReply {
    int64_t term = 0;
    bool success = false;
    int64_t match_index = 0;  // follower's log length after applying, used to fast-forward nextIndex
    // Fast conflict-backtrack hints (Raft paper §5.3 optimization): instead of
    // decrementing nextIndex by one per failed AppendEntries (slow to converge
    // after a long partition), the follower tells the leader exactly where its
    // log diverges so the leader can jump nextIndex directly to the right spot.
    int64_t conflict_term = -1;
    int64_t conflict_index = -1;
    bool ok = false;
};

}  // namespace quorum
