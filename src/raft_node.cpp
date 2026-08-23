#include "quorum/raft_node.hpp"

#include <algorithm>
#include <random>
#include <thread>

namespace quorum {

namespace {
constexpr int kElectionTimeoutMinMs = 150;
constexpr int kElectionTimeoutMaxMs = 300;
constexpr int kHeartbeatIntervalMs = 50;
constexpr int kTickIntervalMs = 10;
}  // namespace

RaftNode::RaftNode(int node_id, int cluster_size, int base_port,
                     std::shared_ptr<PartitionFilter> filter,
                     std::shared_ptr<ElectionLog> election_log)
    : node_id_(node_id),
      cluster_size_(cluster_size),
      next_index_(cluster_size, 1),
      match_index_(cluster_size, 0),
      transport_(node_id, base_port, std::move(filter)),
      election_log_(std::move(election_log)) {}

RaftNode::~RaftNode() { Stop(); }

void RaftNode::Start() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        ResetElectionDeadlineLocked();
    }
    transport_.Start(this);
    running_ = true;
    ticker_thread_ = std::thread(&RaftNode::TickerLoop, this);
}

void RaftNode::Stop() {
    if (!running_.exchange(false)) return;
    transport_.Stop();
    if (ticker_thread_.joinable()) ticker_thread_.join();
}

int64_t RaftNode::RandomElectionTimeoutMs() const {
    static thread_local std::mt19937 rng(std::random_device{}() ^ (node_id_ * 2654435761u));
    std::uniform_int_distribution<int> dist(kElectionTimeoutMinMs, kElectionTimeoutMaxMs);
    return dist(rng);
}

void RaftNode::ResetElectionDeadlineLocked() {
    election_deadline_ =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(RandomElectionTimeoutMs());
}

// ---------------------------------------------------------------------------
// Ticker: the only driver of time-based state transitions. Runs on its own
// thread; election rounds and heartbeat broadcasts are spawned synchronously
// from here (join-before-return, bounded by the transport's RPC timeout) --
// see README for why this project doesn't fire-and-forget replication.
void RaftNode::TickerLoop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kTickIntervalMs));
        if (!running_) return;

        std::unique_lock<std::mutex> lock(mu_);
        auto now = std::chrono::steady_clock::now();

        if (role_ == Role::Leader) {
            if (now - last_heartbeat_sent_ >= std::chrono::milliseconds(kHeartbeatIntervalMs)) {
                last_heartbeat_sent_ = now;
                BroadcastAppendEntriesLocked(lock);
            }
        } else {
            if (now >= election_deadline_) {
                StartElectionLocked(lock);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Elections

void RaftNode::StartElectionLocked(std::unique_lock<std::mutex>& lock) {
    role_ = Role::Candidate;
    current_term_++;
    voted_for_ = node_id_;
    ResetElectionDeadlineLocked();
    int64_t my_term = current_term_;
    int64_t last_log_index = LastLogIndexLocked();
    int64_t last_log_term = TermAtLocked(last_log_index);

    RequestVoteArgs args{my_term, node_id_, last_log_index, last_log_term};

    lock.unlock();

    // Small cluster (this project targets up to a handful of nodes) -> a
    // thread per peer, joined before returning, is simpler and easier to
    // reason about than a thread pool or async I/O loop. See README.
    std::vector<std::thread> voters;
    std::atomic<int> votes{1};  // vote for self
    std::atomic<bool> became_leader{false};

    for (int peer = 0; peer < cluster_size_; ++peer) {
        if (peer == node_id_) continue;
        voters.emplace_back([this, peer, args, my_term, &votes, &became_leader]() {
            RequestVoteReply reply = transport_.SendRequestVote(peer, args);
            if (!reply.ok) return;  // lost message / partitioned / timed out

            std::unique_lock<std::mutex> lock(mu_);
            if (reply.term > current_term_) {
                current_term_ = reply.term;
                role_ = Role::Follower;
                voted_for_ = -1;
                return;
            }
            if (role_ != Role::Candidate || current_term_ != my_term) return;  // stale reply
            if (!reply.vote_granted) return;

            int now_votes = ++votes;
            if (now_votes > cluster_size_ / 2 && role_ == Role::Candidate &&
                current_term_ == my_term && !became_leader.exchange(true)) {
                BecomeLeaderLocked(lock);
            }
        });
    }
    for (auto& t : voters) t.join();

    // BecomeLeaderLocked already fires the first heartbeat round itself, so
    // nothing further to do here.
}

void RaftNode::BecomeLeaderLocked(std::unique_lock<std::mutex>& lock) {
    role_ = Role::Leader;
    leader_id_ = node_id_;
    for (int i = 0; i < cluster_size_; ++i) {
        next_index_[i] = LastLogIndexLocked() + 1;
        match_index_[i] = 0;
    }
    // A no-op entry stamped with the new term. Until an entry from THIS term
    // commits, the leader cannot safely advance commitIndex past entries
    // inherited from earlier terms (Raft paper §5.4.2 / Figure 8) -- without
    // this, a leader that crashes right after being elected can leave the
    // cluster unable to ever commit a previous leader's uncommitted-but-
    // replicated-on-a-majority entry. This was the hardest bug in this
    // project; see README "Hardest bug" section.
    log_.push_back(LogEntry{current_term_, "", "", true});
    match_index_[node_id_] = LastLogIndexLocked();
    last_heartbeat_sent_ = std::chrono::steady_clock::now();
    if (election_log_) election_log_->Record(current_term_, node_id_);

    BroadcastAppendEntriesLocked(lock);
}

// ---------------------------------------------------------------------------
// Replication

void RaftNode::BroadcastAppendEntriesLocked(std::unique_lock<std::mutex>& lock) {
    if (role_ != Role::Leader) return;
    int64_t my_term = current_term_;

    struct PeerCall {
        int peer;
        AppendEntriesArgs args;
        int64_t entries_end_index;  // prev_log_index + entries.size(), i.e. what match_index becomes on success
    };
    std::vector<PeerCall> calls;
    for (int peer = 0; peer < cluster_size_; ++peer) {
        if (peer == node_id_) continue;
        int64_t prev_index = next_index_[peer] - 1;
        AppendEntriesArgs args;
        args.term = my_term;
        args.leader_id = node_id_;
        args.prev_log_index = prev_index;
        args.prev_log_term = TermAtLocked(prev_index);
        args.leader_commit = commit_index_;
        for (int64_t i = prev_index; i < LastLogIndexLocked(); ++i) args.entries.push_back(log_[i]);
        calls.push_back({peer, args, prev_index + static_cast<int64_t>(args.entries.size())});
    }
    lock.unlock();

    std::vector<std::thread> senders;
    for (auto& call : calls) {
        senders.emplace_back([this, call, my_term]() {
            AppendEntriesReply reply = transport_.SendAppendEntries(call.peer, call.args);
            if (!reply.ok) return;

            std::lock_guard<std::mutex> lock(mu_);
            if (reply.term > current_term_) {
                current_term_ = reply.term;
                role_ = Role::Follower;
                voted_for_ = -1;
                return;
            }
            if (role_ != Role::Leader || current_term_ != my_term) return;  // stale reply

            if (reply.success) {
                next_index_[call.peer] = call.entries_end_index + 1;
                match_index_[call.peer] = call.entries_end_index;
                TryAdvanceCommitIndexLocked();
            } else {
                // Fast conflict backtrack (Raft paper §5.3): jump nextIndex to
                // right after the last entry we share with the follower,
                // instead of decrementing by one and retrying every heartbeat.
                if (reply.conflict_term < 0) {
                    next_index_[call.peer] = std::max<int64_t>(1, reply.conflict_index);
                } else {
                    int64_t idx = -1;
                    for (int64_t i = LastLogIndexLocked(); i >= 1; --i) {
                        if (TermAtLocked(i) == reply.conflict_term) {
                            idx = i;
                            break;
                        }
                    }
                    next_index_[call.peer] = (idx >= 0) ? idx + 1 : reply.conflict_index;
                }
                next_index_[call.peer] = std::max<int64_t>(1, next_index_[call.peer]);
            }
        });
    }
    for (auto& t : senders) t.join();

    lock.lock();
}

void RaftNode::TryAdvanceCommitIndexLocked() {
    if (role_ != Role::Leader) return;
    for (int64_t n = LastLogIndexLocked(); n > commit_index_; --n) {
        if (TermAtLocked(n) != current_term_) continue;  // Figure 8 safety
        int count = 1;                                    // count self
        for (int i = 0; i < cluster_size_; ++i) {
            if (i != node_id_ && match_index_[i] >= n) count++;
        }
        if (count > cluster_size_ / 2) {
            commit_index_ = n;
            ApplyCommittedLocked();
            break;
        }
    }
}

void RaftNode::ApplyCommittedLocked() {
    while (last_applied_ < commit_index_) {
        last_applied_++;
        const LogEntry& e = log_[last_applied_ - 1];
        if (!e.is_noop) kv_[e.key] = e.value;
    }
}

// ---------------------------------------------------------------------------
// RPC handlers

RequestVoteReply RaftNode::OnRequestVote(const RequestVoteArgs& args) {
    std::lock_guard<std::mutex> lock(mu_);
    RequestVoteReply reply;
    reply.ok = true;

    if (args.term > current_term_) {
        current_term_ = args.term;
        voted_for_ = -1;
        role_ = Role::Follower;
    }
    reply.term = current_term_;

    if (args.term < current_term_) {
        reply.vote_granted = false;
        return reply;
    }

    int64_t my_last_index = LastLogIndexLocked();
    int64_t my_last_term = TermAtLocked(my_last_index);
    bool candidate_log_ok = (args.last_log_term > my_last_term) ||
                             (args.last_log_term == my_last_term && args.last_log_index >= my_last_index);

    if ((voted_for_ == -1 || voted_for_ == args.candidate_id) && candidate_log_ok) {
        voted_for_ = args.candidate_id;
        reply.vote_granted = true;
        ResetElectionDeadlineLocked();  // granting a vote counts as "heard from a legitimate participant"
    } else {
        reply.vote_granted = false;
    }
    return reply;
}

AppendEntriesReply RaftNode::OnAppendEntries(const AppendEntriesArgs& args) {
    std::lock_guard<std::mutex> lock(mu_);
    AppendEntriesReply reply;
    reply.ok = true;

    if (args.term < current_term_) {
        reply.term = current_term_;
        reply.success = false;
        return reply;
    }

    if (args.term > current_term_) {
        current_term_ = args.term;
        voted_for_ = -1;
    }
    role_ = Role::Follower;  // any AppendEntries at term >= ours means: there IS a legitimate leader
    leader_id_ = args.leader_id;
    ResetElectionDeadlineLocked();
    reply.term = current_term_;

    if (args.prev_log_index > LastLogIndexLocked()) {
        reply.success = false;
        reply.conflict_index = LastLogIndexLocked() + 1;
        reply.conflict_term = -1;
        return reply;
    }
    if (args.prev_log_index > 0 && TermAtLocked(args.prev_log_index) != args.prev_log_term) {
        int64_t conflict_term = TermAtLocked(args.prev_log_index);
        int64_t idx = args.prev_log_index;
        while (idx > 1 && TermAtLocked(idx - 1) == conflict_term) idx--;
        reply.success = false;
        reply.conflict_term = conflict_term;
        reply.conflict_index = idx;
        return reply;
    }

    int64_t index = args.prev_log_index;
    for (const auto& e : args.entries) {
        index++;
        if (index <= LastLogIndexLocked()) {
            if (TermAtLocked(index) != e.term) {
                log_.resize(index - 1);
                log_.push_back(e);
            }
            // else: already have this exact entry, nothing to do
        } else {
            log_.push_back(e);
        }
    }

    if (args.leader_commit > commit_index_) {
        commit_index_ = std::min(args.leader_commit, LastLogIndexLocked());
        ApplyCommittedLocked();
    }

    reply.success = true;
    reply.match_index = LastLogIndexLocked();
    return reply;
}

// ---------------------------------------------------------------------------
// Client API

bool RaftNode::Put(const std::string& key, const std::string& value) {
    int64_t target_index;
    int64_t target_term;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (role_ != Role::Leader) return false;
        LogEntry e{current_term_, key, value, false};
        log_.push_back(e);
        target_index = LastLogIndexLocked();
        target_term = current_term_;
        match_index_[node_id_] = target_index;
    }

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (current_term_ != target_term || role_ != Role::Leader) return false;  // deposed
            if (commit_index_ >= target_index) return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return false;
}

bool RaftNode::Get(const std::string& key, std::string* out_value) {
    // Serves from local applied state. NOT linearizable: a partitioned former
    // leader that hasn't yet stepped down could serve a stale value. A real
    // deployment would need the read-index or lease-read protocol from the
    // Raft paper's extended version -- deliberately out of scope here, see
    // README Limitations.
    std::lock_guard<std::mutex> lock(mu_);
    if (role_ != Role::Leader) return false;
    auto it = kv_.find(key);
    if (it == kv_.end()) return false;
    *out_value = it->second;
    return true;
}

Role RaftNode::GetRole() const {
    std::lock_guard<std::mutex> lock(mu_);
    return role_;
}

int64_t RaftNode::GetCurrentTerm() const {
    std::lock_guard<std::mutex> lock(mu_);
    return current_term_;
}

int RaftNode::GetLeaderId() const {
    std::lock_guard<std::mutex> lock(mu_);
    return leader_id_;
}

int64_t RaftNode::GetCommitIndex() const {
    std::lock_guard<std::mutex> lock(mu_);
    return commit_index_;
}

size_t RaftNode::GetLogLength() const {
    std::lock_guard<std::mutex> lock(mu_);
    return log_.size();
}

std::vector<LogEntry> RaftNode::GetLogSnapshot() const {
    std::lock_guard<std::mutex> lock(mu_);
    return log_;
}

}  // namespace quorum
