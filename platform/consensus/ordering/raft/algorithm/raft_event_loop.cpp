#include "raft_event_loop.h"

#include <glog/logging.h>
#include <chrono>
#include <random>

#include "DafnyRuntime.h"
#include "the_pro.h"
#include "raft.cpp"

#include "platform/consensus/ordering/raft/proto/proposal.pb.h"
#include "platform/proto/resdb.pb.h"

namespace resdb {
namespace raft {

const int timeout_min = 1000;
const int timeout_max = 2000; 

using RaftT = DafnySequence<char>;
using SM = Raft::StateMachine<RaftT>;

struct RaftEventLoop::RaftImpl {
    std::shared_ptr<SM> sm;
};

static RaftT ToRaftT(const std::string& s) {
    DafnySequence<char> seq;
    for (char c : s) {
        seq = seq.concatenate(DafnySequence<char>::Create({c}));
    }
    return seq;
}

static std::string RaftTToString(const RaftT& s) {
    std::string result;
    for (size_t i = 0; i < s.size(); i++) {
        result += s.select(i);
    }
    return result;
}

static const char* RoleName(const Raft::Role& role) {
    if (is_Role_Follower(role)) return "Follower";
    if (is_Role_Candidate(role)) return "Candidate";
    if (is_Role_Leader(role)) return "Leader";
    return "UnknownRole";
}

static const char* WireMessageTypeName(int type) {
    switch (type) {
        case MessageType::AppendEntriesMsg: return "AppendEntriesMsg";
        case MessageType::AppendEntriesResponseMsg: return "AppendEntriesResponseMsg";
        case MessageType::RequestVoteMsg: return "RequestVoteMsg";
        case MessageType::RequestVoteResponseMsg: return "RequestVoteResponseMsg";
        default: return "UnknownWireMsg";
    }
}

static const char* DafnyMessageTypeName(const Types::Message<RaftT>& msg) {
    if (is_Message_AppendEntries(msg)) return "AE";
    if (is_Message_AppendEntriesResponse(msg)) return "AER";
    if (is_Message_RequestVote(msg)) return "RV";
    if (is_Message_RequestVoteResponse(msg)) return "RVR";
    return "?";
}

static std::string StateSummary(const std::shared_ptr<SM>& sm) {
    std::ostringstream oss;
    oss << "role=" << RoleName(sm->role)
        << " term=" << sm->currentTerm
        << " votedFor=" << sm->votedFor
        << " lastLogIndex=" << sm->lastLogIndex
        << " commitIndex=" << sm->commitIndex
        << " lastApplied=" << sm->lastApplied
        << " timeoutGen=" << sm->timeOutGen
        << " quorum=" << sm->quorum
        << " total_num=" << sm->total__num
        << " id=" << sm->id;
    return oss.str();
}

static std::string MessageSummary(const Types::Message<RaftT>& msg) {
    std::ostringstream oss;
    if (is_Message_AppendEntries(msg)) {
        auto& m = std::get<Types::Message_AppendEntries<RaftT>>(msg.v);
        oss << "AE(term=" << m.term
            << ", leaderId=" << m.leaderId
            << ", prevLogIndex=" << m.prevLogIndex
            << ", prevLogTerm=" << m.prevLogTerm
            << ", entries=" << m.entries.size()
            << ", leaderCommitIndex=" << m.leaderCommitIndex
            << ")";
    } else if (is_Message_AppendEntriesResponse(msg)) {
        auto& m = std::get<Types::Message_AppendEntriesResponse<RaftT>>(msg.v);
        oss << "AER(term=" << m.term
            << ", success=" << (m.success ? "true" : "false")
            << ", senderId=" << m.senderId
            << ", lastLogIndex=" << m.lastLogIndex
            << ")";
    } else if (is_Message_RequestVote(msg)) {
        auto& m = std::get<Types::Message_RequestVote<RaftT>>(msg.v);
        oss << "RV(term=" << m.term
            << ", candidateId=" << m.candidateId
            << ", lastLogIndex=" << m.lastLogIndex
            << ", lastLogTerm=" << m.lastLogTerm
            << ")";
    } else if (is_Message_RequestVoteResponse(msg)) {
        auto& m = std::get<Types::Message_RequestVoteResponse<RaftT>>(msg.v);
        oss << "RVR(term=" << m.term
            << ", voteGranted=" << (m.voteGranted ? "true" : "false")
            << ", senderId=" << m.senderId
            << ")";
    }
    return oss.str();
}

static Types::Message<RaftT> ProtoToAE(const AppendEntries& ae) {
    DafnySequence<Types::Entry<RaftT>> entries;
    for (int i = 0; i < ae.entries_size(); i++) {
        auto& e = ae.entries(i);
        entries = entries.concatenate(DafnySequence<Types::Entry<RaftT>>::Create({
            Types::Entry<RaftT>((uint64)e.term(), ToRaftT(e.command()))
        }));
    }
    return Types::Message<RaftT>::create_AppendEntries(
        (uint64)ae.term(), (uint64)ae.leaderid(),
        (uint64)ae.prevlogindex(), (uint64)ae.prevlogterm(),
        entries, (uint64)ae.leadercommitindex()
    );
}

static Types::Message<RaftT> ProtoToAER(const AppendEntriesResponse& aer) {
    return Types::Message<RaftT>::create_AppendEntriesResponse(
        (uint64)aer.term(), aer.success(),
        (uint64)aer.id(), (uint64)aer.lastlogindex()
    );
}

static Types::Message<RaftT> ProtoToRV(const RequestVote& rv) {
    return Types::Message<RaftT>::create_RequestVote(
        (uint64)rv.term(), (uint64)rv.candidateid(),
        (uint64)rv.lastlogindex(), (uint64)rv.lastlogterm()
    );
}

static Types::Message<RaftT> ProtoToRVR(const RequestVoteResponse& rvr) {
    return Types::Message<RaftT>::create_RequestVoteResponse(
        (uint64)rvr.term(), rvr.votegranted(), (uint64)rvr.voterid()
    );
}

static void SendDafnyMessage(const Types::Send<RaftT>& send,
    std::function<int(int, const google::protobuf::Message&, int)>& send_msg) {
    int to = (int)send.to;
    auto& msg = send.msg;
    LOG(INFO) << "EventLoop: outbound "
              << DafnyMessageTypeName(msg)
              << " to=" << to
              << " payload=" << MessageSummary(msg);
    if (is_Message_AppendEntries(msg)) {
        AppendEntries ae;
        auto& m = std::get<Types::Message_AppendEntries<RaftT>>(msg.v);
        ae.set_term(m.term);
        ae.set_leaderid(m.leaderId);
        ae.set_prevlogindex(m.prevLogIndex);
        ae.set_prevlogterm(m.prevLogTerm);
        ae.set_leadercommitindex(m.leaderCommitIndex);
        for (size_t i = 0; i < m.entries.size(); i++) {
            auto e = m.entries.select(i);
            auto* entry = ae.add_entries();
            entry->set_term(e.term);
            entry->set_command(RaftTToString(e.command));
        }
        send_msg(MessageType::AppendEntriesMsg, ae, to);
    } else if (is_Message_AppendEntriesResponse(msg)) {
        AppendEntriesResponse aer;
        auto& m = std::get<Types::Message_AppendEntriesResponse<RaftT>>(msg.v);
        aer.set_term(m.term);
        aer.set_success(m.success);
        aer.set_id(m.senderId);
        aer.set_lastlogindex(m.lastLogIndex);
        send_msg(MessageType::AppendEntriesResponseMsg, aer, to);
    } else if (is_Message_RequestVote(msg)) {
        RequestVote rv;
        auto& m = std::get<Types::Message_RequestVote<RaftT>>(msg.v);
        rv.set_term(m.term);
        rv.set_candidateid(m.candidateId);
        rv.set_lastlogindex(m.lastLogIndex);
        rv.set_lastlogterm(m.lastLogTerm);
        send_msg(MessageType::RequestVoteMsg, rv, to);
    } else if (is_Message_RequestVoteResponse(msg)) {
        RequestVoteResponse rvr;
        auto& m = std::get<Types::Message_RequestVoteResponse<RaftT>>(msg.v);
        rvr.set_term(m.term);
        rvr.set_votegranted(m.voteGranted);
        rvr.set_voterid(m.senderId);
        send_msg(MessageType::RequestVoteResponseMsg, rvr, to);
    }
}

RaftEventLoop::RaftEventLoop(
    int id, int f, int total_num,
    std::function<int(int, const google::protobuf::Message&, int)> send_msg,
    std::function<int(int, const google::protobuf::Message&)> broadcast,
    std::function<int(const google::protobuf::Message&)> commit,
    std::function<void(int, uint64_t)> on_became_leader)
    : id_(id),
      f_(f),
      total_num_(total_num),
      rng_(std::random_device{}()),
      send_msg_(std::move(send_msg)),
      broadcast_(std::move(broadcast)),
      commit_(std::move(commit)),
      on_became_leader_(std::move(on_became_leader)),
      network_queue_("raft_network") {

    impl_ = std::make_unique<RaftImpl>();
    RaftT sentinel = ToRaftT("SENTINEL");
    impl_->sm = Raft::__default::StateMachineFactory<RaftT>(
        (uint64)id, (uint64)f, (uint64)total_num, sentinel);

    LOG(INFO) << "EventLoop: constructed with "
              << "id=" << id_
              << " f=" << f_
              << " total_num=" << total_num_
              << " initialState={" << StateSummary(impl_->sm) << "}";
}

RaftEventLoop::~RaftEventLoop() {
    LOG(INFO) << "EventLoop: shutting down id=" << id_;
    is_stop_ = true;
    if (eventloop_thread_.joinable()) {
        eventloop_thread_.join();
    }
}

void RaftEventLoop::Start() {
    std::uniform_int_distribution<int> dist(timeout_min, timeout_max);
    int timeout_ms = dist(rng_);
    election_timer_ = TimerEntry{
        0,
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms)
    };
    LOG(INFO) << "EventLoop: start id=" << id_
              << " initial election timer gen=0 delay_ms=" << timeout_ms;
    eventloop_thread_ = std::thread(&RaftEventLoop::EventLoopThread, this);
}

void RaftEventLoop::PushNetworkMessage(std::unique_ptr<Request> request) {
    if (request == nullptr) {
        LOG(WARNING) << "EventLoop: PushNetworkMessage received null request";
        return;
    }
    LOG(INFO) << "EventLoop: enqueue wire_msg_type="
              << request->user_type()
              << " (" << WireMessageTypeName(request->user_type()) << ")"
              << " seq=" << request->seq()
              << " sender=" << request->sender_id()
              << " proxy_id=" << request->proxy_id()
              << " bytes=" << request->data().size();
    network_queue_.Push(std::move(request));
}

void RaftEventLoop::CheckTimers() {
    
    auto now = std::chrono::steady_clock::now();
    if (election_timer_.has_value() && now >= election_timer_->expiry) {
        LOG(INFO) << "EventLoop: election timer fired gen=" << election_timer_->gen
                  << " stateBefore={" << StateSummary(impl_->sm) << "}";
        Types::Event<RaftT> e = Types::Event<RaftT>::create_PossibleTimeOut(election_timer_->gen);
        election_timer_.reset();
        HandleAndNotify(e);
        if (Raft::is_Role_Leader(impl_->sm->role) && on_became_leader_) {
            on_became_leader_(id_, (uint64_t)impl_->sm->currentTerm);
        }
    }
    for (auto it = replicate_timers_.begin(); it != replicate_timers_.end(); ) {
        if (now >= it->second.expiry) {
            uint64_t fid = it->first;
            uint64_t gen = it->second.gen;
            LOG(INFO) << "EventLoop: replicate timer fired followerId=" << fid
                      << " gen=" << gen
                      << " stateBefore={" << StateSummary(impl_->sm) << "}";
            it = replicate_timers_.erase(it);
            Types::Event<RaftT> e = Types::Event<RaftT>::create_PossibleHeartBeatNeeded(fid, gen);
            HandleAndNotify(e);
        }
        else {
            ++it;
        }
    }
}

void RaftEventLoop::HandleAndNotify(Types::Event<RaftT> e) {
    bool was_leader = is_Role_Leader(impl_->sm->role);
    ProcessOutput(impl_->sm->HandleEvent(e));
    if (!was_leader && is_Role_Leader(impl_->sm->role) && on_became_leader_) {
        LOG(INFO) << "EventLoop: local leader transition id=" << id_
                  << " term=" << impl_->sm->currentTerm;
        on_became_leader_(id_, (uint64_t)impl_->sm->currentTerm);
    }
}

void RaftEventLoop::ProcessOutput(const Types::Output<RaftT>& output) {
    if (!is_MessageOutput_NoMessage(output.messages)) {
        auto& msgs = std::get<Types::MessageOutput_Messages<RaftT>>(output.messages.v).msgs;
        LOG(INFO) << "EventLoop: output contains " << msgs.size()
                  << " outbound message(s)";
        for (size_t i = 0; i < msgs.size(); i++) {
            auto send = msgs.select(i);
            SendDafnyMessage(send, send_msg_);
        }
    }
    else {
        // LOG(INFO) << "EventLoop: output contains no outbound messages";
    }
    if (!is_TimerOutput_NoTimerChange(output.timer)) {
        auto& changes = std::get<Types::TimerOutput_TimerChanges>(output.timer.v).changes;
        LOG(INFO) << "EventLoop: output contains " << changes.size()
                  << " timer change(s)";
        for (size_t i = 0; i < changes.size(); i++) {
            auto cmd = changes.select(i);
            if (is_TimerCommand_SetElectionTimer(cmd)) {
                std::uniform_int_distribution<int> dist(timeout_min, timeout_max);
                uint64_t gen = std::get<Types::TimerCommand_SetElectionTimer>(cmd.v).gen;
                int timeout_ms = dist(rng_);
                election_timer_ = TimerEntry{
                    gen,
                    std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms)
                };
                LOG(INFO) << "EventLoop: set election timer gen=" << gen
                          << " delay_ms=" << timeout_ms;
            } 
            else if (is_TimerCommand_SetReplicateTimer(cmd)) {
                auto& rt = std::get<Types::TimerCommand_SetReplicateTimer>(cmd.v);
                replicate_timers_[rt.followerId] = TimerEntry{
                    rt.gen,
                    std::chrono::steady_clock::now() + std::chrono::milliseconds(50)
                };
                LOG(INFO) << "EventLoop: set replicate timer followerId="
                          << rt.followerId
                          << " gen=" << rt.gen
                          << " delay_ms=50";
            }
        }
    }
    else {
        // LOG(INFO) << "EventLoop: output contains no timer changes";
    }
    // process commits
    if (is_CommitOutput_CommitAdvanced(output.commit)) {
    auto& ca = std::get<Types::CommitOutput_CommitAdvanced>(output.commit.v);
    LOG(INFO) << "EventLoop: commit advanced from lastApplied="
                  << ca.lastApplied
                  << " to commitIndex=" << ca.commitIndex;
        for (uint64_t i = ca.lastApplied; i <= ca.commitIndex; i++) {
            auto entry = impl_->sm->log.at(i);
            std::string bytes = RaftTToString(entry.command);
            Request req;
            req.ParseFromString(bytes);
            req.set_seq(i);
            commit_(req);
        }
    }
    else {
        // LOG(INFO) << "EventLoop: output contains no commit advancement";
    }
}

void RaftEventLoop::HandleOneRequest(std::unique_ptr<Request> req) {
    LOG(INFO) << "EventLoop: dequeued wire_msg_type=" << req->user_type()
              << " seq=" << req->seq()
              << " sender=" << req->sender_id()
              << " proxy_id=" << req->proxy_id()
              << " bytes=" << req->data().size();

    Types::Event<RaftT> e;

    if (req->user_type() == MessageType::RequestVoteMsg ||
        req->user_type() == MessageType::RequestVoteResponseMsg) {
        LOG(INFO) << "EventLoop: inbound " << WireMessageTypeName(req->user_type())
                << " from sender=" << req->sender_id();
}

    if (req->user_type() == MessageType::AppendEntriesMsg) {
        AppendEntries ae;
        ae.ParseFromString(req->data());
        e = Types::Event<RaftT>::create_ReceiveMessage(ProtoToAE(ae));
    } else if (req->user_type() == MessageType::AppendEntriesResponseMsg) {
        AppendEntriesResponse aer;
        aer.ParseFromString(req->data());
        e = Types::Event<RaftT>::create_ReceiveMessage(ProtoToAER(aer));
    } else if (req->user_type() == MessageType::RequestVoteMsg) {
        RequestVote rv;
        rv.ParseFromString(req->data());
        e = Types::Event<RaftT>::create_ReceiveMessage(ProtoToRV(rv));
    } else if (req->user_type() == MessageType::RequestVoteResponseMsg) {
        RequestVoteResponse rvr;
        rvr.ParseFromString(req->data());
        e = Types::Event<RaftT>::create_ReceiveMessage(ProtoToRVR(rvr));
    } else if (req->user_type() == 0) {
        std::string bytes;
        req->SerializeToString(&bytes);
        e = Types::Event<RaftT>::create_ReceiveTransaction(ToRaftT(bytes));
    } else {
        LOG(WARNING) << "EventLoop: ignoring unknown wire_msg_type="
                     << req->user_type();
        return;
    }
    HandleAndNotify(e);
}

void RaftEventLoop::EventLoopThread() {
    LOG(INFO) << "EventLoop: thread started id=" << id_;
    while (!is_stop_) {
        static const int MESSAGE_QUOTA = 10;
        int processed = 0;
        while (processed < MESSAGE_QUOTA) {
            auto req = network_queue_.Pop(0);
            if (!req) break;
            HandleOneRequest(std::move(req));
            processed++;
        }
        CheckTimers();
        if (processed == 0) {
            auto req = network_queue_.Pop(5);  // wait up to 5 ms
            if (req) {
                HandleOneRequest(std::move(req));
            }
        }
    }
    LOG(INFO) << "EventLoop: thread exiting id=" << id_;
}

}
}