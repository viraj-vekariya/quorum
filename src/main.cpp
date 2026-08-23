// Demo CLI: boots an N-node Raft cluster as threads within this one process
// (real TCP between them on 127.0.0.1), and lets you PUT/GET against it.
//
// Usage:
//   ./quorum_demo [cluster_size]
//   > put foo bar
//   > get foo
//   > status
//   > quit

#include <iostream>
#include <memory>
#include <sstream>
#include <thread>
#include <vector>

#include "quorum/partition_filter.hpp"
#include "quorum/raft_node.hpp"

using namespace quorum;

namespace {
const char* RoleName(Role r) {
    switch (r) {
        case Role::Follower: return "follower";
        case Role::Candidate: return "candidate";
        case Role::Leader: return "leader";
    }
    return "?";
}

// A node's Role can be stale if it was Stop()'d while it happened to be
// leader -- Stop() simulates the node going unresponsive, not the node
// finding out it's been superseded, since that would require it to still be
// able to receive RPCs. Preferring the highest current_term_ among all
// self-reported leaders is a real (if imperfect) way real Raft clients
// resolve this: a genuinely-current leader always has the highest term of
// any node claiming leadership, because becoming leader requires winning an
// election, which always bumps the term.
int FindLeader(std::vector<std::unique_ptr<RaftNode>>& nodes) {
    int best_id = -1;
    int64_t best_term = -1;
    for (size_t i = 0; i < nodes.size(); ++i) {
        if (nodes[i]->GetRole() == Role::Leader && nodes[i]->GetCurrentTerm() > best_term) {
            best_term = nodes[i]->GetCurrentTerm();
            best_id = static_cast<int>(i);
        }
    }
    return best_id;
}
}  // namespace

int main(int argc, char** argv) {
    int cluster_size = 5;
    if (argc > 1) cluster_size = std::stoi(argv[1]);
    const int base_port = 9000;

    auto filter = std::make_shared<PartitionFilter>();
    std::vector<std::unique_ptr<RaftNode>> nodes;
    for (int i = 0; i < cluster_size; ++i) {
        nodes.push_back(std::make_unique<RaftNode>(i, cluster_size, base_port, filter));
    }
    for (auto& n : nodes) n->Start();

    std::cout << "quorum: " << cluster_size << "-node Raft cluster started on 127.0.0.1:"
              << base_port << "-" << (base_port + cluster_size - 1) << "\n";
    std::cout << "waiting for initial leader election...\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::string line;
    std::cout << "> " << std::flush;
    while (std::getline(std::cin, line)) {
        std::istringstream is(line);
        std::string cmd;
        is >> cmd;

        if (cmd == "quit" || cmd == "exit") {
            break;
        } else if (cmd == "status") {
            for (size_t i = 0; i < nodes.size(); ++i) {
                auto& n = nodes[i];
                std::cout << "  node " << i << " [term " << n->GetCurrentTerm() << "] "
                          << RoleName(n->GetRole()) << " commitIndex=" << n->GetCommitIndex()
                          << " logLen=" << n->GetLogLength() << "\n";
            }
        } else if (cmd == "put") {
            std::string key, value;
            is >> key >> value;
            int leader = FindLeader(nodes);
            if (leader < 0) {
                std::cout << "  no leader currently known, try again shortly\n";
            } else if (nodes[leader]->Put(key, value)) {
                std::cout << "  OK (committed via node " << leader << ")\n";
            } else {
                std::cout << "  FAILED (leader changed mid-write, retry)\n";
            }
        } else if (cmd == "get") {
            std::string key;
            is >> key;
            int leader = FindLeader(nodes);
            std::string value;
            if (leader >= 0 && nodes[leader]->Get(key, &value)) {
                std::cout << "  " << value << "\n";
            } else {
                std::cout << "  (not found, or no leader)\n";
            }
        } else if (cmd == "kill") {
            int id;
            is >> id;
            if (id >= 0 && id < cluster_size) {
                nodes[id]->Stop();
                std::cout << "  node " << id << " stopped\n";
            }
        } else if (cmd == "revive") {
            int id;
            is >> id;
            if (id >= 0 && id < cluster_size) {
                nodes[id]->Start();
                std::cout << "  node " << id << " restarted\n";
            }
        } else if (!cmd.empty()) {
            std::cout << "  commands: put <k> <v> | get <k> | status | kill <id> | revive <id> | quit\n";
        }
        std::cout << "> " << std::flush;
    }

    for (auto& n : nodes) n->Stop();
    return 0;
}
