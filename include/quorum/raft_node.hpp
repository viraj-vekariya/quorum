#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "quorum/election_log.hpp"
#include "quorum/partition_filter.hpp"
#include "quorum/rpc_handler.hpp"
#include "quorum/transport.hpp"
#include "quorum/types.hpp"

namespace quorum {

class RaftNode : public RpcHandler {
public:
    RaftNode(int node_id, int cluster_size, int base_port, std::shared_ptr<PartitionFilter> filter,
              std::shared_ptr<ElectionLog> election_log = nullptr);
    ~RaftNode();

    void Start();  // binds transport, starts the ticker thread
    void Stop();   // simulates the node going unresponsive (see README limitations)

    // Client-facing API. Must be called on whichever node the caller currently
    // believes is the leader (see GetLeaderId()); returns false immediately if
    // this node isn't the leader, or after a bounded wait if the entry never
    // commits (e.g. this node was deposed before replicating it).
    bool Put(const std::string& key, const std::string& value);

    // Linearizable read via the ReadIndex protocol: before serving from
    // local state, this runs a live AppendEntries round and requires a
    // majority to acknowledge at the term this call started with. Only a
    // leader that is STILL provably a majority-recognized leader for this
    // exact term, right now, will answer -- a partitioned former leader that
    // hasn't yet noticed a higher term will fail to get quorum and return
    // false rather than serve a stale value. See README for the guarantee
    // this does and does not provide.
    bool Get(const std::string& key, std::string* out_value);

    // Test/demo introspection.
    Role GetRole() const;
    int64_t GetCurrentTerm() const;
    int GetLeaderId() const;
    int64_t GetCommitIndex() const;
    size_t GetLogLength() const;
    std::vector<LogEntry> GetLogSnapshot() const;

    // RpcHandler
    RequestVoteReply OnRequestVote(const RequestVoteArgs& args) override;
    AppendEntriesReply OnAppendEntries(const AppendEntriesArgs& args) override;

private:
    void TickerLoop();
    void StartElectionLocked(std::unique_lock<std::mutex>& lock);
    // Returns the number of nodes (including self) that acknowledged this
    // round at `current_term_` as it was when the round started -- i.e. a
    // live quorum-confirmation count, not just "heartbeat sent". Used both
    // as an ordinary heartbeat (return value ignored) and, by Get(), as the
    // ReadIndex leadership-confirmation round.
    int BroadcastAppendEntriesLocked(std::unique_lock<std::mutex>& lock);
    void BecomeLeaderLocked(std::unique_lock<std::mutex>& lock);
    void TryAdvanceCommitIndexLocked();
    void ApplyCommittedLocked();
    void ResetElectionDeadlineLocked();
    int64_t RandomElectionTimeoutMs() const;

    int64_t LastLogIndexLocked() const { return static_cast<int64_t>(log_.size()); }
    int64_t TermAtLocked(int64_t index) const {
        if (index <= 0 || index > static_cast<int64_t>(log_.size())) return 0;
        return log_[index - 1].term;
    }

    const int node_id_;
    const int cluster_size_;

    mutable std::mutex mu_;
    Role role_ = Role::Follower;
    int64_t current_term_ = 0;
    int voted_for_ = -1;
    std::vector<LogEntry> log_;  // 1-indexed conceptually: log_[i-1] is index i
    int64_t commit_index_ = 0;
    int64_t last_applied_ = 0;
    int leader_id_ = -1;

    std::vector<int64_t> next_index_;   // leader-only, sized cluster_size_
    std::vector<int64_t> match_index_;  // leader-only, sized cluster_size_

    std::unordered_map<std::string, std::string> kv_;

    std::chrono::steady_clock::time_point election_deadline_;
    std::chrono::steady_clock::time_point last_heartbeat_sent_;

    Transport transport_;
    std::shared_ptr<ElectionLog> election_log_;
    std::thread ticker_thread_;
    std::atomic<bool> running_{false};
};

}  // namespace quorum
