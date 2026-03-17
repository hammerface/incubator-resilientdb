#pragma once

#include <memory>
#include <thread>
#include <atomic>
#include <functional>
#include <optional>
#include <unordered_map>
#include <chrono>
#include <random>

#include "platform/common/queue/lock_free_queue.h"
#include "platform/consensus/ordering/raft/proto/proposal.pb.h"
#include "platform/proto/resdb.pb.h"
#include "platform/networkstrate/replica_communicator.h"

#include "DafnyRuntime.h"
#include "the_pro.h"

namespace resdb {
namespace raft {

using RaftT = DafnySequence<char>;

class RaftEventLoop {
public:
    RaftEventLoop(
        int id, int f, int total_num,
        std::function<int(int, const google::protobuf::Message&, int)> send_msg,
        std::function<int(int, const google::protobuf::Message&)> broadcast,
        std::function<int(const google::protobuf::Message&)> commit,
        std::function<void(int, uint64_t)> on_became_leader
    );
    ~RaftEventLoop();

    void Start();
    void PushNetworkMessage(std::unique_ptr<Request> request);

private:
    struct RaftImpl;
    std::unique_ptr<RaftImpl> impl_;
    

    void HandleOneRequest(std::unique_ptr<resdb::Request>);
    void EventLoopThread();
    void ProcessOutput(const Types::Output<RaftT>& output);
    void CheckTimers();
    void HandleAndNotify(Types::Event<RaftT> e);
    

    struct TimerEntry {
        uint64_t gen;
        std::chrono::steady_clock::time_point expiry;
    };

    int id_, f_, total_num_;
    std::mt19937 rng_;
    std::function<int(int, const google::protobuf::Message&, int)> send_msg_;
    std::function<int(int, const google::protobuf::Message&)> broadcast_;
    std::function<int(const google::protobuf::Message&)> commit_;
    std::function<void(int, uint64_t)> on_became_leader_;

    LockFreeQueue<Request> network_queue_;
    std::optional<TimerEntry> election_timer_;
    std::unordered_map<uint64_t, TimerEntry> replicate_timers_;

    std::thread eventloop_thread_;
    std::atomic<bool> is_stop_{false};
};

}  // namespace raft
}  // namespace resdb