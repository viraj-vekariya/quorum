#include "quorum/transport.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <iostream>
#include <optional>
#include <sstream>
#include <vector>

namespace quorum {
namespace {

// --- wire format -----------------------------------------------------------
// Deliberately a plain line-oriented text protocol (see README): the point of
// this project is Raft correctness, not payload serialization, so keys/values
// are restricted to non-whitespace, colon-free tokens and everything fits on
// one newline-terminated line.

std::string EncodeEntry(const LogEntry& e) {
    std::ostringstream os;
    os << e.term << ":" << (e.is_noop ? 1 : 0) << ":"
       << (e.key.empty() ? "-" : e.key) << ":"
       << (e.value.empty() ? "-" : e.value);
    return os.str();
}

LogEntry DecodeEntry(const std::string& tok) {
    LogEntry e;
    std::istringstream is(tok);
    std::string term_s, noop_s, key_s, val_s;
    std::getline(is, term_s, ':');
    std::getline(is, noop_s, ':');
    std::getline(is, key_s, ':');
    std::getline(is, val_s, ':');
    e.term = std::stoll(term_s);
    e.is_noop = (noop_s == "1");
    e.key = (key_s == "-") ? "" : key_s;
    e.value = (val_s == "-") ? "" : val_s;
    return e;
}

std::string EncodeRequestVoteArgs(const RequestVoteArgs& a) {
    std::ostringstream os;
    os << "RVREQ " << a.term << " " << a.candidate_id << " "
       << a.last_log_index << " " << a.last_log_term;
    return os.str();
}

std::string EncodeRequestVoteReply(const RequestVoteReply& r) {
    std::ostringstream os;
    os << "RVRESP " << r.term << " " << (r.vote_granted ? 1 : 0);
    return os.str();
}

std::string EncodeAppendEntriesArgs(const AppendEntriesArgs& a) {
    std::ostringstream os;
    os << "AEREQ " << a.term << " " << a.leader_id << " " << a.prev_log_index
       << " " << a.prev_log_term << " " << a.leader_commit << " "
       << a.entries.size();
    for (const auto& e : a.entries) os << " " << EncodeEntry(e);
    return os.str();
}

std::string EncodeAppendEntriesReply(const AppendEntriesReply& r) {
    std::ostringstream os;
    os << "AERESP " << r.term << " " << (r.success ? 1 : 0) << " "
       << r.match_index << " " << r.conflict_term << " " << r.conflict_index;
    return os.str();
}

// --- socket helpers ----------------------------------------------------

bool SendLine(int fd, const std::string& line) {
    std::string msg = line + "\n";
    size_t sent = 0;
    while (sent < msg.size()) {
        ssize_t n = ::send(fd, msg.data() + sent, msg.size() - sent, 0);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

// Reads until '\n' or timeout/EOF. Caller sets SO_RCVTIMEO beforehand for the
// timeout to actually bound this.
std::optional<std::string> RecvLine(int fd) {
    std::string buf;
    char c;
    while (true) {
        ssize_t n = ::recv(fd, &c, 1, 0);
        if (n <= 0) return std::nullopt;  // timeout, error, or peer closed
        if (c == '\n') return buf;
        buf.push_back(c);
        if (buf.size() > 1u << 20) return std::nullopt;  // sanity cap
    }
}

void SetTimeout(int fd, int ms) {
    struct timeval tv;
    tv.tv_sec = ms / 1000;
    tv.tv_usec = (ms % 1000) * 1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

constexpr int kRpcTimeoutMs = 100;

}  // namespace

Transport::Transport(int node_id, int base_port, std::shared_ptr<PartitionFilter> filter)
    : node_id_(node_id), base_port_(base_port), filter_(std::move(filter)) {}

Transport::~Transport() { Stop(); }

void Transport::Start(RpcHandler* handler) {
    handler_ = handler;
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = htons(static_cast<uint16_t>(PortFor(node_id_)));

    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "[node " << node_id_ << "] bind failed on port "
                  << PortFor(node_id_) << ": " << strerror(errno) << "\n";
        std::abort();
    }
    ::listen(listen_fd_, 64);

    running_ = true;
    listen_thread_ = std::thread(&Transport::ListenLoop, this);
}

void Transport::Stop() {
    if (!running_.exchange(false)) return;
    if (listen_fd_ >= 0) {
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    if (listen_thread_.joinable()) listen_thread_.join();
}

void Transport::ListenLoop() {
    while (running_) {
        struct sockaddr_in client_addr {};
        socklen_t len = sizeof(client_addr);
        int client_fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &len);
        if (client_fd < 0) {
            if (!running_) return;  // Stop() closed the socket out from under us
            continue;
        }
        // Handled inline rather than spawned per-connection: Raft's message
        // rate at this cluster size is low, and handling on the accept thread
        // keeps ordering simple. HandleConnection returns quickly since the
        // handler dispatch itself is non-blocking (state mutation only).
        HandleConnection(client_fd);
    }
}

void Transport::HandleConnection(int client_fd) {
    SetTimeout(client_fd, kRpcTimeoutMs);
    auto line = RecvLine(client_fd);
    if (!line) {
        ::close(client_fd);
        return;
    }
    std::istringstream is(*line);
    std::string kind;
    is >> kind;

    if (kind == "RVREQ") {
        RequestVoteArgs args;
        is >> args.term >> args.candidate_id >> args.last_log_index >> args.last_log_term;
        // Partition check on the RECEIVE side too: cuts both directions of the
        // simulated link even though this is a real accepted TCP connection.
        if (!filter_->allowed(args.candidate_id, node_id_)) {
            ::close(client_fd);
            return;
        }
        RequestVoteReply reply = handler_->OnRequestVote(args);
        SendLine(client_fd, EncodeRequestVoteReply(reply));
    } else if (kind == "AEREQ") {
        AppendEntriesArgs args;
        size_t n_entries = 0;
        is >> args.term >> args.leader_id >> args.prev_log_index >> args.prev_log_term >>
            args.leader_commit >> n_entries;
        args.entries.reserve(n_entries);
        for (size_t i = 0; i < n_entries; ++i) {
            std::string tok;
            is >> tok;
            args.entries.push_back(DecodeEntry(tok));
        }
        if (!filter_->allowed(args.leader_id, node_id_)) {
            ::close(client_fd);
            return;
        }
        AppendEntriesReply reply = handler_->OnAppendEntries(args);
        SendLine(client_fd, EncodeAppendEntriesReply(reply));
    }
    ::close(client_fd);
}

RequestVoteReply Transport::SendRequestVote(int peer_id, const RequestVoteArgs& args) {
    RequestVoteReply reply;
    if (!filter_->allowed(node_id_, peer_id)) return reply;  // ok == false

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    SetTimeout(fd, kRpcTimeoutMs);
    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = htons(static_cast<uint16_t>(PortFor(peer_id)));

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return reply;
    }
    if (!SendLine(fd, EncodeRequestVoteArgs(args))) {
        ::close(fd);
        return reply;
    }
    auto line = RecvLine(fd);
    ::close(fd);
    if (!line) return reply;

    std::istringstream is(*line);
    std::string kind;
    int voted;
    is >> kind >> reply.term >> voted;
    reply.vote_granted = (voted == 1);
    reply.ok = true;
    return reply;
}

AppendEntriesReply Transport::SendAppendEntries(int peer_id, const AppendEntriesArgs& args) {
    AppendEntriesReply reply;
    if (!filter_->allowed(node_id_, peer_id)) return reply;  // ok == false

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    SetTimeout(fd, kRpcTimeoutMs);
    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = htons(static_cast<uint16_t>(PortFor(peer_id)));

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return reply;
    }
    if (!SendLine(fd, EncodeAppendEntriesArgs(args))) {
        ::close(fd);
        return reply;
    }
    auto line = RecvLine(fd);
    ::close(fd);
    if (!line) return reply;

    std::istringstream is(*line);
    std::string kind;
    int success;
    is >> kind >> reply.term >> success >> reply.match_index >> reply.conflict_term >>
        reply.conflict_index;
    reply.success = (success == 1);
    reply.ok = true;
    return reply;
}

}  // namespace quorum
