#pragma once

#include "quorum/types.hpp"

namespace quorum {

// Implemented by RaftNode. The transport layer parses wire messages and
// dispatches into these — it knows nothing about Raft's state machine.
class RpcHandler {
public:
    virtual ~RpcHandler() = default;
    virtual RequestVoteReply OnRequestVote(const RequestVoteArgs& args) = 0;
    virtual AppendEntriesReply OnAppendEntries(const AppendEntriesArgs& args) = 0;
};

}  // namespace quorum
