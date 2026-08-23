#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <thread>

#include "quorum/partition_filter.hpp"
#include "quorum/rpc_handler.hpp"
#include "quorum/types.hpp"

namespace quorum {

// Real TCP RPC transport. Each node owns one Transport bound to
// 127.0.0.1:base_port+node_id and listens for incoming RPCs on a background
// thread; outgoing RPCs open a short-lived connection per call (no pooling —
// simplicity over throughput, this isn't the bottleneck at Raft's message
// rate) with a bounded socket timeout so a dead/partitioned peer fails fast
// instead of hanging a caller.
class Transport {
public:
    Transport(int node_id, int base_port, std::shared_ptr<PartitionFilter> filter);
    ~Transport();

    // Starts the listener thread and begins dispatching incoming RPCs to `handler`.
    void Start(RpcHandler* handler);
    void Stop();

    // Blocking calls with a socket-level timeout. `reply.ok == false` means the
    // RPC failed (timeout, refused, or partitioned) — the caller must treat
    // this exactly like Raft treats any lost message: no state change, retry
    // on the next tick.
    RequestVoteReply SendRequestVote(int peer_id, const RequestVoteArgs& args);
    AppendEntriesReply SendAppendEntries(int peer_id, const AppendEntriesArgs& args);

    int node_id() const { return node_id_; }

private:
    int PortFor(int peer_id) const { return base_port_ + peer_id; }
    void ListenLoop();
    void HandleConnection(int client_fd);

    int node_id_;
    int base_port_;
    std::shared_ptr<PartitionFilter> filter_;
    RpcHandler* handler_ = nullptr;

    // Accessed from both the owning thread (Stop()) and the listener thread
    // (ListenLoop()'s accept() call) -- must be atomic, a plain int here is a
    // genuine data race (caught by ThreadSanitizer during development; see
    // README "Hardest bug").
    std::atomic<int> listen_fd_{-1};
    std::thread listen_thread_;
    std::atomic<bool> running_{false};
};

}  // namespace quorum
