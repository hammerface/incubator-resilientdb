/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "platform/consensus/ordering/raft/framework/consensus.h"

#include <glog/logging.h>
#include <unistd.h>

#include "common/utils/utils.h"
#include "platform/consensus/ordering/raft/proto/proposal.pb.h"
#include "platform/proto/resdb.pb.h"

namespace resdb {
namespace raft {

Consensus::Consensus(const ResDBConfig& config,
                     std::unique_ptr<TransactionManager> executor)
    : common::Consensus(config, std::move(executor)) {
  //LOG(INFO) << "JIM -> " << __FUNCTION__ << ": In consensus constructor";
  int total_replicas = config_.GetReplicaNum();
  int f = (total_replicas - 1) / 2;

  Init();

  if (config_.GetPublicKeyCertificateInfo()
          .public_key()
          .public_key_info()
          .type() != CertificateKeyInfo::CLIENT) {
    event_loop_ = std::make_unique<RaftEventLoop>(config_.GetSelfInfo().id(), f, total_replicas,
        [this](int type, const google::protobuf::Message& msg, int node_id) {
          return SendMsg(type, msg, node_id);
        },
        [this](int type, const google::protobuf::Message& msg) {
            return Broadcast(type, msg);
        },
        [this](const google::protobuf::Message& msg) {
            return CommitMsg(msg);
        },
        [this](int leader_id, uint64_t term) {
          DirectToLeader dtl;
          dtl.set_term(term);
          dtl.set_leaderid(leader_id);
          auto clients = replica_communicator_->GetClientReplicas();
          LOG(INFO) << "on_became_leader: sending DTL leader=" << leader_id 
              << " term=" << term 
              << " num_clients=" << clients.size();
          for (const auto& client : clients) {
              LOG(INFO) << "on_became_leader: sending to client id=" << client.id();
              SendMsg(MessageType::DirectToLeaderMsg, dtl, client.id());
          }
        });
    
    event_loop_->Start();
  }
}

static const char* WireMessageTypeName(int type) {
    switch (type) {
        case MessageType::AppendEntriesMsg: return "AppendEntriesMsg";
        case MessageType::AppendEntriesResponseMsg: return "AppendEntriesResponseMsg";
        case MessageType::RequestVoteMsg: return "RequestVoteMsg";
        case MessageType::RequestVoteResponseMsg: return "RequestVoteResponseMsg";
        case MessageType::DirectToLeaderMsg: return "DirectToLeaderMsg";
        default: return "UnknownWireMsg";
    }
}

int Consensus::ProcessCustomConsensus(std::unique_ptr<Request> request) {
  LOG(INFO) << "Consensus: received message type " << WireMessageTypeName(request->user_type());
  if (request->user_type() == MessageType::DirectToLeaderMsg) {
    std::unique_ptr<DirectToLeader> dtl = std::make_unique<resdb::raft::DirectToLeader>();
    if (!dtl->ParseFromString(request->data())) {
      LOG(ERROR) << "parse proposal fail";
      assert(1 == 0);
      return -1;
    }
    performance_manager_->SetPrimary(dtl->leaderid());
    return 0;
  }
  event_loop_->PushNetworkMessage(std::move(request));
  return 0;
}

int Consensus::ProcessNewTransaction(std::unique_ptr<Request> request) {
    event_loop_->PushNetworkMessage(std::move(request));
    return 0;
}

int Consensus::CommitMsg(const google::protobuf::Message& msg) {
  auto* req = dynamic_cast<const Request*>(&msg);
  if (!req) {
    LOG(INFO) << "JIM -> " << __FUNCTION__ << ": Failed to cast Message to Request";
    return -1;
  }
  auto execReq = std::make_unique<Request>(*req);
  LOG(INFO) << "CommitMsg: seq=" << req->seq() 
          << " proxy_id=" << req->proxy_id()
          << " user_seq=" << req->user_seq();
  transaction_executor_->Commit(std::move(execReq));
  return 0;
}

void Consensus::SendDirectToLeader(int leader_id, uint64_t term) {
    DirectToLeader dtl;
    dtl.set_term(term);
    dtl.set_leaderid(leader_id);
    for (const auto& client : replica_communicator_->GetClientReplicas()) {
        SendMsg(MessageType::DirectToLeaderMsg, dtl, client.id());
    }
}

}  // namespace raft
}  // namespace resdb
