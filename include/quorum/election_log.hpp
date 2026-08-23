#pragma once

#include <mutex>
#include <utility>
#include <vector>

namespace quorum {

// Records every (term, node_id) leadership transition as it happens.
// Production Raft libraries expose something equivalent (e.g.
// hashicorp/raft's leaderCh) so callers can react to leadership changes;
// here it doubles as the mechanism tests use to verify Election Safety
// (never two different leaders in the same term) across an entire run,
// since polling GetRole() periodically could miss a leader that won and
// lost leadership between two polls.
class ElectionLog {
public:
    void Record(int64_t term, int node_id) {
        std::lock_guard<std::mutex> lock(mu_);
        events_.emplace_back(term, node_id);
    }

    std::vector<std::pair<int64_t, int>> Snapshot() const {
        std::lock_guard<std::mutex> lock(mu_);
        return events_;
    }

private:
    mutable std::mutex mu_;
    std::vector<std::pair<int64_t, int>> events_;
};

}  // namespace quorum
