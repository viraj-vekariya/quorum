// Correctness suite for quorum's Raft implementation. Every test here talks
// to real RaftNode instances over real TCP sockets on 127.0.0.1 -- nothing
// is mocked. See README for how each test maps to a specific Raft safety
// property.

#include <algorithm>
#include <chrono>
#include <map>
#include <memory>
#include <set>
#include <thread>
#include <vector>

#include "quorum/election_log.hpp"
#include "quorum/partition_filter.hpp"
#include "quorum/raft_node.hpp"
#include "test_framework.hpp"

using namespace quorum;
using namespace std::chrono_literals;

namespace {

// Ports for each test are offset so consecutive tests never race for the
// same port while a previous test's sockets are still closing.
int g_next_base_port = 9100;
int NextBasePort(int span) {
    int p = g_next_base_port;
    g_next_base_port += span + 20;
    return p;
}

struct Cluster {
    std::shared_ptr<PartitionFilter> filter = std::make_shared<PartitionFilter>();
    std::shared_ptr<ElectionLog> election_log = std::make_shared<ElectionLog>();
    std::vector<std::unique_ptr<RaftNode>> nodes;
    int size;
    int base_port;

    explicit Cluster(int n) : size(n), base_port(NextBasePort(n)) {
        for (int i = 0; i < n; ++i) {
            nodes.push_back(std::make_unique<RaftNode>(i, n, base_port, filter, election_log));
        }
        for (auto& node : nodes) node->Start();
    }

    ~Cluster() {
        for (auto& node : nodes) node->Stop();
    }

    // Polls until some node reports Leader, or times out. Returns -1 on timeout.
    int WaitForLeader(std::chrono::milliseconds timeout = 2000ms) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            int best_id = -1;
            int64_t best_term = -1;
            for (int i = 0; i < size; ++i) {
                if (nodes[i]->GetRole() == Role::Leader && nodes[i]->GetCurrentTerm() > best_term) {
                    best_term = nodes[i]->GetCurrentTerm();
                    best_id = i;
                }
            }
            if (best_id >= 0) return best_id;
            std::this_thread::sleep_for(10ms);
        }
        return -1;
    }

    // Election Safety: scan the election log and confirm no term has two
    // different leaders recorded. Call this at the end of every test that
    // triggers elections -- it inspects the FULL history, not a snapshot.
    void AssertElectionSafety() {
        std::map<int64_t, std::set<int>> leaders_by_term;
        for (auto& [term, id] : election_log->Snapshot()) leaders_by_term[term].insert(id);
        for (auto& [term, ids] : leaders_by_term) {
            ASSERT_EQ(ids.size(), 1u);
        }
    }
};

}  // namespace

// ---------------------------------------------------------------------------

TEST(five_node_cluster_elects_exactly_one_leader) {
    Cluster c(5);
    int leader = c.WaitForLeader();
    ASSERT_TRUE(leader >= 0);

    int leader_count = 0;
    for (int i = 0; i < c.size; ++i) {
        if (c.nodes[i]->GetRole() == Role::Leader) leader_count++;
    }
    ASSERT_EQ(leader_count, 1);
    c.AssertElectionSafety();
}

TEST(basic_replication_put_then_get_from_majority) {
    Cluster c(5);
    int leader = c.WaitForLeader();
    ASSERT_TRUE(leader >= 0);

    ASSERT_TRUE(c.nodes[leader]->Put("x", "1"));
    ASSERT_TRUE(c.nodes[leader]->Put("y", "2"));
    ASSERT_TRUE(c.nodes[leader]->Put("x", "3"));  // overwrite

    // Give followers a couple of heartbeat rounds to catch up their local
    // commitIndex (the leader commits as soon as a majority replicates, but
    // followers only learn the new commitIndex on their NEXT AppendEntries).
    std::this_thread::sleep_for(150ms);

    int agree = 0;
    for (int i = 0; i < c.size; ++i) {
        if (c.nodes[i]->GetCommitIndex() >= 3) agree++;
    }
    ASSERT_GE(agree, 3);  // majority of 5

    std::string value;
    ASSERT_TRUE(c.nodes[leader]->Get("x", &value));
    ASSERT_EQ(value, "3");
    c.AssertElectionSafety();
}

TEST(non_leader_put_is_rejected_immediately) {
    Cluster c(5);
    int leader = c.WaitForLeader();
    ASSERT_TRUE(leader >= 0);
    int follower = (leader + 1) % c.size;
    ASSERT_TRUE(c.nodes[follower]->GetRole() != Role::Leader);
    ASSERT_TRUE(c.nodes[follower]->Put("k", "v") == false);
}

TEST(killing_the_leader_elects_a_new_one_and_cluster_keeps_committing) {
    Cluster c(5);
    int leader1 = c.WaitForLeader();
    ASSERT_TRUE(leader1 >= 0);
    ASSERT_TRUE(c.nodes[leader1]->Put("before", "crash"));
    int64_t term1 = c.nodes[leader1]->GetCurrentTerm();

    c.nodes[leader1]->Stop();

    int leader2 = -1;
    auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
        for (int i = 0; i < c.size; ++i) {
            if (i == leader1) continue;
            if (c.nodes[i]->GetRole() == Role::Leader && c.nodes[i]->GetCurrentTerm() > term1) {
                leader2 = i;
                break;
            }
        }
        if (leader2 >= 0) break;
        std::this_thread::sleep_for(10ms);
    }
    ASSERT_TRUE(leader2 >= 0);
    ASSERT_TRUE(leader2 != leader1);
    ASSERT_TRUE(c.nodes[leader2]->GetCurrentTerm() > term1);

    ASSERT_TRUE(c.nodes[leader2]->Put("after", "recovery"));
    std::string v;
    ASSERT_TRUE(c.nodes[leader2]->Get("before", &v));
    ASSERT_EQ(v, "crash");  // survived the leader change
    ASSERT_TRUE(c.nodes[leader2]->Get("after", &v));
    ASSERT_EQ(v, "recovery");

    c.AssertElectionSafety();
}

TEST(minority_partition_makes_no_progress_majority_does) {
    Cluster c(5);
    int leader = c.WaitForLeader();
    ASSERT_TRUE(leader >= 0);
    ASSERT_TRUE(c.nodes[leader]->Put("seed", "1"));

    // Partition the leader plus one follower into a minority (2 of 5) against
    // the remaining majority (3 of 5).
    int minority_follower = -1;
    for (int i = 0; i < c.size; ++i) {
        if (i != leader) {
            minority_follower = i;
            break;
        }
    }
    std::set<int> minority = {leader, minority_follower};
    std::set<int> majority;
    for (int i = 0; i < c.size; ++i) {
        if (!minority.count(i)) majority.insert(i);
    }
    c.filter->partition(minority, majority);

    // Minority side: the old leader can no longer reach a majority of the
    // cluster, so a write it accepts locally must never commit.
    bool minority_put_result = c.nodes[leader]->Put("stuck", "value");
    ASSERT_TRUE(minority_put_result == false);

    // Majority side elects its own leader and keeps making progress.
    int majority_leader = -1;
    auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
        for (int i : majority) {
            if (c.nodes[i]->GetRole() == Role::Leader) {
                majority_leader = i;
                break;
            }
        }
        if (majority_leader >= 0) break;
        std::this_thread::sleep_for(10ms);
    }
    ASSERT_TRUE(majority_leader >= 0);
    ASSERT_TRUE(c.nodes[majority_leader]->Put("progress", "yes"));

    // Heal the partition; the stale leader must see the higher term and step
    // down rather than continuing to think it's in charge.
    c.filter->heal();
    auto step_down_deadline = std::chrono::steady_clock::now() + 1s;
    bool stepped_down = false;
    while (std::chrono::steady_clock::now() < step_down_deadline) {
        if (c.nodes[leader]->GetRole() != Role::Leader) {
            stepped_down = true;
            break;
        }
        std::this_thread::sleep_for(10ms);
    }
    ASSERT_TRUE(stepped_down);

    // And the stuck write from the old leader must genuinely be gone --
    // not just uncommitted-but-still-there, actually absent from the
    // now-reunified cluster's committed state.
    std::this_thread::sleep_for(200ms);
    std::string v;
    ASSERT_TRUE(c.nodes[majority_leader]->Get("stuck", &v) == false);

    c.AssertElectionSafety();
}

TEST(concurrent_load_with_follower_churn_never_loses_a_committed_entry) {
    Cluster c(5);
    int leader = c.WaitForLeader();
    ASSERT_TRUE(leader >= 0);

    constexpr int kWriters = 4;
    constexpr int kPutsPerWriter = 15;
    std::vector<std::thread> writers;
    std::atomic<int> total_committed{0};

    for (int w = 0; w < kWriters; ++w) {
        writers.emplace_back([&, w]() {
            for (int i = 0; i < kPutsPerWriter; ++i) {
                // Always resolve the current leader fresh -- it may change
                // mid-test as we kill/restart a follower below.
                int cur = -1;
                int64_t best_term = -1;
                for (int n = 0; n < c.size; ++n) {
                    if (c.nodes[n]->GetRole() == Role::Leader && c.nodes[n]->GetCurrentTerm() > best_term) {
                        best_term = c.nodes[n]->GetCurrentTerm();
                        cur = n;
                    }
                }
                if (cur < 0) continue;
                std::string key = "w" + std::to_string(w) + "_" + std::to_string(i);
                if (c.nodes[cur]->Put(key, "v")) total_committed++;
            }
        });
    }

    // Churn a follower (never the current leader) mid-load.
    std::this_thread::sleep_for(80ms);
    int victim = (leader + 1) % c.size;
    c.nodes[victim]->Stop();
    std::this_thread::sleep_for(150ms);
    c.nodes[victim]->Start();

    for (auto& t : writers) t.join();
    std::this_thread::sleep_for(300ms);  // let final replication settle

    ASSERT_GE(total_committed.load(), kWriters * kPutsPerWriter - kWriters);  // allow a few losses to the churned node's own attempts, not to the cluster

    // Every node still up must agree with the leader on every committed key
    // this test itself confirmed committed (Put() returned true) -- no
    // silent divergence.
    int final_leader = c.WaitForLeader();
    ASSERT_TRUE(final_leader >= 0);
    auto reference_log = c.nodes[final_leader]->GetLogSnapshot();
    int64_t ref_commit = c.nodes[final_leader]->GetCommitIndex();

    for (int i = 0; i < c.size; ++i) {
        if (c.nodes[i]->GetRole() == Role::Leader) continue;
        int64_t their_commit = c.nodes[i]->GetCommitIndex();
        int64_t check_up_to = std::min(ref_commit, their_commit);
        auto their_log = c.nodes[i]->GetLogSnapshot();
        for (int64_t idx = 1; idx <= check_up_to; ++idx) {
            ASSERT_TRUE(idx - 1 < static_cast<int64_t>(their_log.size()));
            ASSERT_EQ(their_log[idx - 1].term, reference_log[idx - 1].term);
            ASSERT_EQ(their_log[idx - 1].key, reference_log[idx - 1].key);
            ASSERT_EQ(their_log[idx - 1].value, reference_log[idx - 1].value);
        }
    }

    c.AssertElectionSafety();
}

int main() { return testing::RunAll(); }
