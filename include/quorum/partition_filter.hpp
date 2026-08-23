#pragma once

#include <mutex>
#include <set>

namespace quorum {

// Test-only fault injection sitting ABOVE the real TCP transport: every node
// still opens a real socket, connects, and serializes a real message. This
// filter just decides, at the application level, whether a given (from, to)
// attempt is allowed to proceed — used by tests to simulate a network
// partition without actually manipulating OS-level routing/firewall rules.
// One instance is shared by every node in a test process.
class PartitionFilter {
public:
    // Split the cluster into two groups; nodes in different groups cannot
    // reach each other in either direction. An empty call clears any split.
    void partition(std::set<int> group_a, std::set<int> group_b) {
        std::lock_guard<std::mutex> lock(mu_);
        group_a_ = std::move(group_a);
        group_b_ = std::move(group_b);
        active_ = true;
    }

    void heal() {
        std::lock_guard<std::mutex> lock(mu_);
        active_ = false;
        group_a_.clear();
        group_b_.clear();
    }

    bool allowed(int from, int to) const {
        std::lock_guard<std::mutex> lock(mu_);
        if (!active_) return true;
        bool from_in_a = group_a_.count(from) > 0;
        bool to_in_a = group_a_.count(to) > 0;
        bool from_in_b = group_b_.count(from) > 0;
        bool to_in_b = group_b_.count(to) > 0;
        if (from_in_a && to_in_b) return false;
        if (from_in_b && to_in_a) return false;
        return true;
    }

private:
    mutable std::mutex mu_;
    bool active_ = false;
    std::set<int> group_a_;
    std::set<int> group_b_;
};

}  // namespace quorum
