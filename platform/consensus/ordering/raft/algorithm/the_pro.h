// Dafny program the_program compiled into a Cpp header file
#pragma once
#include "DafnyRuntime.h"
namespace Types  {
  class __default;
  class class_uint64;
  template <typename T>
struct Entry;
  template <typename T>
struct Message;
  template <typename T>
struct Event;
  template <typename T>
struct Send;
  template <typename T>
struct MessageOutput;
  
struct TimerCommand;
  
struct TimerOutput;
  
struct CommitOutput;
  template <typename T>
struct Output;
}// end of namespace Types  declarations
namespace Raft  {
  class __default;
  
struct Role;
  
struct TermRelation;
  
struct ReplicationState;
  template <typename T>
  class StateMachine;
}// end of namespace Raft  declarations
namespace _module  {
}// end of namespace _module  declarations
namespace Types  {
  template <typename T>
struct Entry {
    uint64 term;
    T command;
    Entry(uint64 term, T command) : term (term),  command (command) {}
    Entry();
    friend bool operator==(const Entry &left, const Entry &right) {
      	return true		&& left.term == right.term
      		&& left.command == right.command
      ;
    }
    friend bool operator!=(const Entry &left, const Entry &right) { return !(left == right); } 
  };
  template <typename T>
inline bool is_Entry(const struct Entry<T> d) { (void) d; return true; }
  template <typename T>
struct Message_AppendEntries {
    uint64 term;
    uint64 leaderId;
    uint64 prevLogIndex;
    uint64 prevLogTerm;
    DafnySequence<Types::Entry <T> > entries;
    uint64 leaderCommitIndex;
    friend bool operator==(const Message_AppendEntries &left, const Message_AppendEntries &right) { 
    	return true 		&& left.term == right.term
    		&& left.leaderId == right.leaderId
    		&& left.prevLogIndex == right.prevLogIndex
    		&& left.prevLogTerm == right.prevLogTerm
    		&& left.entries == right.entries
    		&& left.leaderCommitIndex == right.leaderCommitIndex
    ;
}
    friend bool operator!=(const Message_AppendEntries &left, const Message_AppendEntries &right) { return !(left == right); } 
  };
  template <typename T>
struct Message_AppendEntriesResponse {
    uint64 term;
    bool success;
    uint64 senderId;
    uint64 lastLogIndex;
    friend bool operator==(const Message_AppendEntriesResponse &left, const Message_AppendEntriesResponse &right) { 
    	return true 		&& left.term == right.term
    		&& left.success == right.success
    		&& left.senderId == right.senderId
    		&& left.lastLogIndex == right.lastLogIndex
    ;
}
    friend bool operator!=(const Message_AppendEntriesResponse &left, const Message_AppendEntriesResponse &right) { return !(left == right); } 
  };
  template <typename T>
struct Message_RequestVote {
    uint64 term;
    uint64 candidateId;
    uint64 lastLogIndex;
    uint64 lastLogTerm;
    friend bool operator==(const Message_RequestVote &left, const Message_RequestVote &right) { 
    	return true 		&& left.term == right.term
    		&& left.candidateId == right.candidateId
    		&& left.lastLogIndex == right.lastLogIndex
    		&& left.lastLogTerm == right.lastLogTerm
    ;
}
    friend bool operator!=(const Message_RequestVote &left, const Message_RequestVote &right) { return !(left == right); } 
  };
  template <typename T>
struct Message_RequestVoteResponse {
    uint64 term;
    bool voteGranted;
    uint64 senderId;
    friend bool operator==(const Message_RequestVoteResponse &left, const Message_RequestVoteResponse &right) { 
    	return true 		&& left.term == right.term
    		&& left.voteGranted == right.voteGranted
    		&& left.senderId == right.senderId
    ;
}
    friend bool operator!=(const Message_RequestVoteResponse &left, const Message_RequestVoteResponse &right) { return !(left == right); } 
  };
  template <typename T>
struct Message {
    std::variant<Message_AppendEntries<T>, Message_AppendEntriesResponse<T>, Message_RequestVote<T>, Message_RequestVoteResponse<T>> v;
    static Message create_AppendEntries(uint64 term, uint64 leaderId, uint64 prevLogIndex, uint64 prevLogTerm, DafnySequence<Types::Entry <T> > entries, uint64 leaderCommitIndex) {
      Message<T> COMPILER_result;
      Message_AppendEntries<T> COMPILER_result_subStruct;
      COMPILER_result_subStruct.term = term;
      COMPILER_result_subStruct.leaderId = leaderId;
      COMPILER_result_subStruct.prevLogIndex = prevLogIndex;
      COMPILER_result_subStruct.prevLogTerm = prevLogTerm;
      COMPILER_result_subStruct.entries = entries;
      COMPILER_result_subStruct.leaderCommitIndex = leaderCommitIndex;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    static Message create_AppendEntriesResponse(uint64 term, bool success, uint64 senderId, uint64 lastLogIndex) {
      Message<T> COMPILER_result;
      Message_AppendEntriesResponse<T> COMPILER_result_subStruct;
      COMPILER_result_subStruct.term = term;
      COMPILER_result_subStruct.success = success;
      COMPILER_result_subStruct.senderId = senderId;
      COMPILER_result_subStruct.lastLogIndex = lastLogIndex;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    static Message create_RequestVote(uint64 term, uint64 candidateId, uint64 lastLogIndex, uint64 lastLogTerm) {
      Message<T> COMPILER_result;
      Message_RequestVote<T> COMPILER_result_subStruct;
      COMPILER_result_subStruct.term = term;
      COMPILER_result_subStruct.candidateId = candidateId;
      COMPILER_result_subStruct.lastLogIndex = lastLogIndex;
      COMPILER_result_subStruct.lastLogTerm = lastLogTerm;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    static Message create_RequestVoteResponse(uint64 term, bool voteGranted, uint64 senderId) {
      Message<T> COMPILER_result;
      Message_RequestVoteResponse<T> COMPILER_result_subStruct;
      COMPILER_result_subStruct.term = term;
      COMPILER_result_subStruct.voteGranted = voteGranted;
      COMPILER_result_subStruct.senderId = senderId;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    Message();
    ~Message() {}
    Message(const Message &other) {
      v = other.v;
    }
    Message& operator=(const Message other) {
      v = other.v;
      return *this;
    }
    bool is_Message_AppendEntries() const { return std::holds_alternative<Message_AppendEntries<T>>(v); }
    bool is_Message_AppendEntriesResponse() const { return std::holds_alternative<Message_AppendEntriesResponse<T>>(v); }
    bool is_Message_RequestVote() const { return std::holds_alternative<Message_RequestVote<T>>(v); }
    bool is_Message_RequestVoteResponse() const { return std::holds_alternative<Message_RequestVoteResponse<T>>(v); }
    friend bool operator==(const Message &left, const Message &right) { 
    	return left.v == right.v;
}
    uint64 dtor_term() {
      if (is_Message_AppendEntries()) { return std::get<Message_AppendEntries<T>>(v).term; }
      if (is_Message_AppendEntriesResponse()) { return std::get<Message_AppendEntriesResponse<T>>(v).term; }
      if (is_Message_RequestVote()) { return std::get<Message_RequestVote<T>>(v).term; }
      return std::get<Message_RequestVoteResponse<T>>(v).term; 
    }
    uint64 dtor_leaderId() {
      return std::get<Message_AppendEntries<T>>(v).leaderId; 
    }
    uint64 dtor_prevLogIndex() {
      return std::get<Message_AppendEntries<T>>(v).prevLogIndex; 
    }
    uint64 dtor_prevLogTerm() {
      return std::get<Message_AppendEntries<T>>(v).prevLogTerm; 
    }
    DafnySequence<Types::Entry <T> > dtor_entries() {
      return std::get<Message_AppendEntries<T>>(v).entries; 
    }
    uint64 dtor_leaderCommitIndex() {
      return std::get<Message_AppendEntries<T>>(v).leaderCommitIndex; 
    }
    bool dtor_success() {
      return std::get<Message_AppendEntriesResponse<T>>(v).success; 
    }
    uint64 dtor_senderId() {
      if (is_Message_AppendEntriesResponse()) { return std::get<Message_AppendEntriesResponse<T>>(v).senderId; }
      return std::get<Message_RequestVoteResponse<T>>(v).senderId; 
    }
    uint64 dtor_lastLogIndex() {
      if (is_Message_AppendEntriesResponse()) { return std::get<Message_AppendEntriesResponse<T>>(v).lastLogIndex; }
      return std::get<Message_RequestVote<T>>(v).lastLogIndex; 
    }
    uint64 dtor_candidateId() {
      return std::get<Message_RequestVote<T>>(v).candidateId; 
    }
    uint64 dtor_lastLogTerm() {
      return std::get<Message_RequestVote<T>>(v).lastLogTerm; 
    }
    bool dtor_voteGranted() {
      return std::get<Message_RequestVoteResponse<T>>(v).voteGranted; 
    }
    friend bool operator!=(const Message &left, const Message &right) { return !(left == right); } 
  };
  template <typename T>
inline bool is_Message_AppendEntries(const struct Message<T> d);
  template <typename T>
inline bool is_Message_AppendEntriesResponse(const struct Message<T> d);
  template <typename T>
inline bool is_Message_RequestVote(const struct Message<T> d);
  template <typename T>
inline bool is_Message_RequestVoteResponse(const struct Message<T> d);
  template <typename T>
struct Event_ReceiveTransaction {
    T request;
    friend bool operator==(const Event_ReceiveTransaction &left, const Event_ReceiveTransaction &right) { 
    	return true 		&& left.request == right.request
    ;
}
    friend bool operator!=(const Event_ReceiveTransaction &left, const Event_ReceiveTransaction &right) { return !(left == right); } 
  };
  template <typename T>
struct Event_ReceiveMessage {
    Types::Message <T>  message;
    friend bool operator==(const Event_ReceiveMessage &left, const Event_ReceiveMessage &right) { 
    	return true 		&& left.message == right.message
    ;
}
    friend bool operator!=(const Event_ReceiveMessage &left, const Event_ReceiveMessage &right) { return !(left == right); } 
  };
  template <typename T>
struct Event_PossibleTimeOut {
    uint64 timeOutGenSnapshot;
    friend bool operator==(const Event_PossibleTimeOut &left, const Event_PossibleTimeOut &right) { 
    	return true 		&& left.timeOutGenSnapshot == right.timeOutGenSnapshot
    ;
}
    friend bool operator!=(const Event_PossibleTimeOut &left, const Event_PossibleTimeOut &right) { return !(left == right); } 
  };
  template <typename T>
struct Event_PossibleHeartBeatNeeded {
    uint64 followerId;
    uint64 heartBeatGenSnapshot;
    friend bool operator==(const Event_PossibleHeartBeatNeeded &left, const Event_PossibleHeartBeatNeeded &right) { 
    	return true 		&& left.followerId == right.followerId
    		&& left.heartBeatGenSnapshot == right.heartBeatGenSnapshot
    ;
}
    friend bool operator!=(const Event_PossibleHeartBeatNeeded &left, const Event_PossibleHeartBeatNeeded &right) { return !(left == right); } 
  };
  template <typename T>
struct Event {
    std::variant<Event_ReceiveTransaction<T>, Event_ReceiveMessage<T>, Event_PossibleTimeOut<T>, Event_PossibleHeartBeatNeeded<T>> v;
    static Event create_ReceiveTransaction(T request) {
      Event<T> COMPILER_result;
      Event_ReceiveTransaction<T> COMPILER_result_subStruct;
      COMPILER_result_subStruct.request = request;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    static Event create_ReceiveMessage(Types::Message <T>  message) {
      Event<T> COMPILER_result;
      Event_ReceiveMessage<T> COMPILER_result_subStruct;
      COMPILER_result_subStruct.message = message;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    static Event create_PossibleTimeOut(uint64 timeOutGenSnapshot) {
      Event<T> COMPILER_result;
      Event_PossibleTimeOut<T> COMPILER_result_subStruct;
      COMPILER_result_subStruct.timeOutGenSnapshot = timeOutGenSnapshot;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    static Event create_PossibleHeartBeatNeeded(uint64 followerId, uint64 heartBeatGenSnapshot) {
      Event<T> COMPILER_result;
      Event_PossibleHeartBeatNeeded<T> COMPILER_result_subStruct;
      COMPILER_result_subStruct.followerId = followerId;
      COMPILER_result_subStruct.heartBeatGenSnapshot = heartBeatGenSnapshot;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    Event();
    ~Event() {}
    Event(const Event &other) {
      v = other.v;
    }
    Event& operator=(const Event other) {
      v = other.v;
      return *this;
    }
    bool is_Event_ReceiveTransaction() const { return std::holds_alternative<Event_ReceiveTransaction<T>>(v); }
    bool is_Event_ReceiveMessage() const { return std::holds_alternative<Event_ReceiveMessage<T>>(v); }
    bool is_Event_PossibleTimeOut() const { return std::holds_alternative<Event_PossibleTimeOut<T>>(v); }
    bool is_Event_PossibleHeartBeatNeeded() const { return std::holds_alternative<Event_PossibleHeartBeatNeeded<T>>(v); }
    friend bool operator==(const Event &left, const Event &right) { 
    	return left.v == right.v;
}
    T dtor_request() {
      return std::get<Event_ReceiveTransaction<T>>(v).request; 
    }
    Types::Message <T>  dtor_message() {
      return std::get<Event_ReceiveMessage<T>>(v).message; 
    }
    uint64 dtor_timeOutGenSnapshot() {
      return std::get<Event_PossibleTimeOut<T>>(v).timeOutGenSnapshot; 
    }
    uint64 dtor_followerId() {
      return std::get<Event_PossibleHeartBeatNeeded<T>>(v).followerId; 
    }
    uint64 dtor_heartBeatGenSnapshot() {
      return std::get<Event_PossibleHeartBeatNeeded<T>>(v).heartBeatGenSnapshot; 
    }
    friend bool operator!=(const Event &left, const Event &right) { return !(left == right); } 
  };
  template <typename T>
inline bool is_Event_ReceiveTransaction(const struct Event<T> d);
  template <typename T>
inline bool is_Event_ReceiveMessage(const struct Event<T> d);
  template <typename T>
inline bool is_Event_PossibleTimeOut(const struct Event<T> d);
  template <typename T>
inline bool is_Event_PossibleHeartBeatNeeded(const struct Event<T> d);
  template <typename T>
struct Send {
    uint64 to;
    Types::Message <T>  msg;
    Send(uint64 to, Types::Message <T>  msg) : to (to),  msg (msg) {}
    Send();
    friend bool operator==(const Send &left, const Send &right) {
      	return true		&& left.to == right.to
      		&& left.msg == right.msg
      ;
    }
    friend bool operator!=(const Send &left, const Send &right) { return !(left == right); } 
  };
  template <typename T>
inline bool is_Send(const struct Send<T> d) { (void) d; return true; }
  template <typename T>
struct MessageOutput_Messages {
    DafnySequence<Types::Send <T> > msgs;
    friend bool operator==(const MessageOutput_Messages &left, const MessageOutput_Messages &right) { 
    	return true 		&& left.msgs == right.msgs
    ;
}
    friend bool operator!=(const MessageOutput_Messages &left, const MessageOutput_Messages &right) { return !(left == right); } 
  };
  template <typename T>
struct MessageOutput_NoMessage {
    friend bool operator==(const MessageOutput_NoMessage &left, const MessageOutput_NoMessage &right) { 
    (void)left; (void) right;
    	return true ;
}
    friend bool operator!=(const MessageOutput_NoMessage &left, const MessageOutput_NoMessage &right) { return !(left == right); } 
  };
  template <typename T>
struct MessageOutput {
    std::variant<MessageOutput_Messages<T>, MessageOutput_NoMessage<T>> v;
    static MessageOutput create_Messages(DafnySequence<Types::Send <T> > msgs) {
      MessageOutput<T> COMPILER_result;
      MessageOutput_Messages<T> COMPILER_result_subStruct;
      COMPILER_result_subStruct.msgs = msgs;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    static MessageOutput create_NoMessage() {
      MessageOutput<T> COMPILER_result;
      MessageOutput_NoMessage<T> COMPILER_result_subStruct;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    MessageOutput();
    ~MessageOutput() {}
    MessageOutput(const MessageOutput &other) {
      v = other.v;
    }
    MessageOutput& operator=(const MessageOutput other) {
      v = other.v;
      return *this;
    }
    bool is_MessageOutput_Messages() const { return std::holds_alternative<MessageOutput_Messages<T>>(v); }
    bool is_MessageOutput_NoMessage() const { return std::holds_alternative<MessageOutput_NoMessage<T>>(v); }
    friend bool operator==(const MessageOutput &left, const MessageOutput &right) { 
    	return left.v == right.v;
}
    DafnySequence<Types::Send <T> > dtor_msgs() {
      return std::get<MessageOutput_Messages<T>>(v).msgs; 
    }
    friend bool operator!=(const MessageOutput &left, const MessageOutput &right) { return !(left == right); } 
  };
  template <typename T>
inline bool is_MessageOutput_Messages(const struct MessageOutput<T> d);
  template <typename T>
inline bool is_MessageOutput_NoMessage(const struct MessageOutput<T> d);
  
struct TimerCommand_SetElectionTimer {
    uint64 gen;
    friend bool operator==(const TimerCommand_SetElectionTimer &left, const TimerCommand_SetElectionTimer &right) { 
    	return true 		&& left.gen == right.gen
    ;
}
    friend bool operator!=(const TimerCommand_SetElectionTimer &left, const TimerCommand_SetElectionTimer &right) { return !(left == right); } 
  };
  
struct TimerCommand_SetReplicateTimer {
    uint64 followerId;
    uint64 gen;
    friend bool operator==(const TimerCommand_SetReplicateTimer &left, const TimerCommand_SetReplicateTimer &right) { 
    	return true 		&& left.followerId == right.followerId
    		&& left.gen == right.gen
    ;
}
    friend bool operator!=(const TimerCommand_SetReplicateTimer &left, const TimerCommand_SetReplicateTimer &right) { return !(left == right); } 
  };
  
struct TimerCommand {
    std::variant<TimerCommand_SetElectionTimer, TimerCommand_SetReplicateTimer> v;
    static TimerCommand create_SetElectionTimer(uint64 gen) {
      TimerCommand COMPILER_result;
      TimerCommand_SetElectionTimer COMPILER_result_subStruct;
      COMPILER_result_subStruct.gen = gen;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    static TimerCommand create_SetReplicateTimer(uint64 followerId, uint64 gen) {
      TimerCommand COMPILER_result;
      TimerCommand_SetReplicateTimer COMPILER_result_subStruct;
      COMPILER_result_subStruct.followerId = followerId;
      COMPILER_result_subStruct.gen = gen;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    TimerCommand();
    ~TimerCommand() {}
    TimerCommand(const TimerCommand &other) {
      v = other.v;
    }
    TimerCommand& operator=(const TimerCommand other) {
      v = other.v;
      return *this;
    }
    bool is_TimerCommand_SetElectionTimer() const { return std::holds_alternative<TimerCommand_SetElectionTimer>(v); }
    bool is_TimerCommand_SetReplicateTimer() const { return std::holds_alternative<TimerCommand_SetReplicateTimer>(v); }
    friend bool operator==(const TimerCommand &left, const TimerCommand &right) { 
    	return left.v == right.v;
}
    uint64 dtor_gen() {
      if (is_TimerCommand_SetElectionTimer()) { return std::get<TimerCommand_SetElectionTimer>(v).gen; }
      return std::get<TimerCommand_SetReplicateTimer>(v).gen; 
    }
    uint64 dtor_followerId() {
      return std::get<TimerCommand_SetReplicateTimer>(v).followerId; 
    }
    friend bool operator!=(const TimerCommand &left, const TimerCommand &right) { return !(left == right); } 
  };
  
inline bool is_TimerCommand_SetElectionTimer(const struct TimerCommand d);
  
inline bool is_TimerCommand_SetReplicateTimer(const struct TimerCommand d);
  
struct TimerOutput_TimerChanges {
    DafnySequence<Types::TimerCommand> changes;
    friend bool operator==(const TimerOutput_TimerChanges &left, const TimerOutput_TimerChanges &right) { 
    	return true 		&& left.changes == right.changes
    ;
}
    friend bool operator!=(const TimerOutput_TimerChanges &left, const TimerOutput_TimerChanges &right) { return !(left == right); } 
  };
  
struct TimerOutput_NoTimerChange {
    friend bool operator==(const TimerOutput_NoTimerChange &left, const TimerOutput_NoTimerChange &right) { 
    (void)left; (void) right;
    	return true ;
}
    friend bool operator!=(const TimerOutput_NoTimerChange &left, const TimerOutput_NoTimerChange &right) { return !(left == right); } 
  };
  
struct TimerOutput {
    std::variant<TimerOutput_TimerChanges, TimerOutput_NoTimerChange> v;
    static TimerOutput create_TimerChanges(DafnySequence<Types::TimerCommand> changes) {
      TimerOutput COMPILER_result;
      TimerOutput_TimerChanges COMPILER_result_subStruct;
      COMPILER_result_subStruct.changes = changes;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    static TimerOutput create_NoTimerChange() {
      TimerOutput COMPILER_result;
      TimerOutput_NoTimerChange COMPILER_result_subStruct;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    TimerOutput();
    ~TimerOutput() {}
    TimerOutput(const TimerOutput &other) {
      v = other.v;
    }
    TimerOutput& operator=(const TimerOutput other) {
      v = other.v;
      return *this;
    }
    bool is_TimerOutput_TimerChanges() const { return std::holds_alternative<TimerOutput_TimerChanges>(v); }
    bool is_TimerOutput_NoTimerChange() const { return std::holds_alternative<TimerOutput_NoTimerChange>(v); }
    friend bool operator==(const TimerOutput &left, const TimerOutput &right) { 
    	return left.v == right.v;
}
    DafnySequence<Types::TimerCommand> dtor_changes() {
      return std::get<TimerOutput_TimerChanges>(v).changes; 
    }
    friend bool operator!=(const TimerOutput &left, const TimerOutput &right) { return !(left == right); } 
  };
  
inline bool is_TimerOutput_TimerChanges(const struct TimerOutput d);
  
inline bool is_TimerOutput_NoTimerChange(const struct TimerOutput d);
  
struct CommitOutput_CommitAdvanced {
    uint64 lastApplied;
    uint64 commitIndex;
    friend bool operator==(const CommitOutput_CommitAdvanced &left, const CommitOutput_CommitAdvanced &right) { 
    	return true 		&& left.lastApplied == right.lastApplied
    		&& left.commitIndex == right.commitIndex
    ;
}
    friend bool operator!=(const CommitOutput_CommitAdvanced &left, const CommitOutput_CommitAdvanced &right) { return !(left == right); } 
  };
  
struct CommitOutput_NoCommit {
    friend bool operator==(const CommitOutput_NoCommit &left, const CommitOutput_NoCommit &right) { 
    (void)left; (void) right;
    	return true ;
}
    friend bool operator!=(const CommitOutput_NoCommit &left, const CommitOutput_NoCommit &right) { return !(left == right); } 
  };
  
struct CommitOutput {
    std::variant<CommitOutput_CommitAdvanced, CommitOutput_NoCommit> v;
    static CommitOutput create_CommitAdvanced(uint64 lastApplied, uint64 commitIndex) {
      CommitOutput COMPILER_result;
      CommitOutput_CommitAdvanced COMPILER_result_subStruct;
      COMPILER_result_subStruct.lastApplied = lastApplied;
      COMPILER_result_subStruct.commitIndex = commitIndex;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    static CommitOutput create_NoCommit() {
      CommitOutput COMPILER_result;
      CommitOutput_NoCommit COMPILER_result_subStruct;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    CommitOutput();
    ~CommitOutput() {}
    CommitOutput(const CommitOutput &other) {
      v = other.v;
    }
    CommitOutput& operator=(const CommitOutput other) {
      v = other.v;
      return *this;
    }
    bool is_CommitOutput_CommitAdvanced() const { return std::holds_alternative<CommitOutput_CommitAdvanced>(v); }
    bool is_CommitOutput_NoCommit() const { return std::holds_alternative<CommitOutput_NoCommit>(v); }
    friend bool operator==(const CommitOutput &left, const CommitOutput &right) { 
    	return left.v == right.v;
}
    uint64 dtor_lastApplied() {
      return std::get<CommitOutput_CommitAdvanced>(v).lastApplied; 
    }
    uint64 dtor_commitIndex() {
      return std::get<CommitOutput_CommitAdvanced>(v).commitIndex; 
    }
    friend bool operator!=(const CommitOutput &left, const CommitOutput &right) { return !(left == right); } 
  };
  
inline bool is_CommitOutput_CommitAdvanced(const struct CommitOutput d);
  
inline bool is_CommitOutput_NoCommit(const struct CommitOutput d);
  template <typename T>
struct Output {
    Types::MessageOutput <T>  messages;
    Types::TimerOutput timer;
    Types::CommitOutput commit;
    Output(Types::MessageOutput <T>  messages, Types::TimerOutput timer, Types::CommitOutput commit) : messages (messages),  timer (timer),  commit (commit) {}
    Output();
    friend bool operator==(const Output &left, const Output &right) {
      	return true		&& left.messages == right.messages
      		&& left.timer == right.timer
      		&& left.commit == right.commit
      ;
    }
    friend bool operator!=(const Output &left, const Output &right) { return !(left == right); } 
  };
  template <typename T>
inline bool is_Output(const struct Output<T> d) { (void) d; return true; }
}// end of namespace Types  datatype declarations
namespace Raft  {
  
struct Role_Follower {
    friend bool operator==(const Role_Follower &left, const Role_Follower &right) { 
    (void)left; (void) right;
    	return true ;
}
    friend bool operator!=(const Role_Follower &left, const Role_Follower &right) { return !(left == right); } 
  };
  
struct Role_Candidate {
    friend bool operator==(const Role_Candidate &left, const Role_Candidate &right) { 
    (void)left; (void) right;
    	return true ;
}
    friend bool operator!=(const Role_Candidate &left, const Role_Candidate &right) { return !(left == right); } 
  };
  
struct Role_Leader {
    friend bool operator==(const Role_Leader &left, const Role_Leader &right) { 
    (void)left; (void) right;
    	return true ;
}
    friend bool operator!=(const Role_Leader &left, const Role_Leader &right) { return !(left == right); } 
  };
  
struct Role {
    std::variant<Role_Follower, Role_Candidate, Role_Leader> v;
    static Role create_Follower() {
      Role COMPILER_result;
      Role_Follower COMPILER_result_subStruct;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    static Role create_Candidate() {
      Role COMPILER_result;
      Role_Candidate COMPILER_result_subStruct;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    static Role create_Leader() {
      Role COMPILER_result;
      Role_Leader COMPILER_result_subStruct;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    Role();
    ~Role() {}
    Role(const Role &other) {
      v = other.v;
    }
    Role& operator=(const Role other) {
      v = other.v;
      return *this;
    }
    bool is_Role_Follower() const { return std::holds_alternative<Role_Follower>(v); }
    bool is_Role_Candidate() const { return std::holds_alternative<Role_Candidate>(v); }
    bool is_Role_Leader() const { return std::holds_alternative<Role_Leader>(v); }
    friend bool operator==(const Role &left, const Role &right) { 
    	return left.v == right.v;
}
    friend bool operator!=(const Role &left, const Role &right) { return !(left == right); } 
  };
  
inline bool is_Role_Follower(const struct Role d);
  
inline bool is_Role_Candidate(const struct Role d);
  
inline bool is_Role_Leader(const struct Role d);
  
struct TermRelation_Stale {
    friend bool operator==(const TermRelation_Stale &left, const TermRelation_Stale &right) { 
    (void)left; (void) right;
    	return true ;
}
    friend bool operator!=(const TermRelation_Stale &left, const TermRelation_Stale &right) { return !(left == right); } 
  };
  
struct TermRelation_Current {
    friend bool operator==(const TermRelation_Current &left, const TermRelation_Current &right) { 
    (void)left; (void) right;
    	return true ;
}
    friend bool operator!=(const TermRelation_Current &left, const TermRelation_Current &right) { return !(left == right); } 
  };
  
struct TermRelation_New {
    friend bool operator==(const TermRelation_New &left, const TermRelation_New &right) { 
    (void)left; (void) right;
    	return true ;
}
    friend bool operator!=(const TermRelation_New &left, const TermRelation_New &right) { return !(left == right); } 
  };
  
struct TermRelation {
    std::variant<TermRelation_Stale, TermRelation_Current, TermRelation_New> v;
    static TermRelation create_Stale() {
      TermRelation COMPILER_result;
      TermRelation_Stale COMPILER_result_subStruct;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    static TermRelation create_Current() {
      TermRelation COMPILER_result;
      TermRelation_Current COMPILER_result_subStruct;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    static TermRelation create_New() {
      TermRelation COMPILER_result;
      TermRelation_New COMPILER_result_subStruct;
      COMPILER_result.v = COMPILER_result_subStruct;
      return COMPILER_result;
    }
    TermRelation();
    ~TermRelation() {}
    TermRelation(const TermRelation &other) {
      v = other.v;
    }
    TermRelation& operator=(const TermRelation other) {
      v = other.v;
      return *this;
    }
    bool is_TermRelation_Stale() const { return std::holds_alternative<TermRelation_Stale>(v); }
    bool is_TermRelation_Current() const { return std::holds_alternative<TermRelation_Current>(v); }
    bool is_TermRelation_New() const { return std::holds_alternative<TermRelation_New>(v); }
    friend bool operator==(const TermRelation &left, const TermRelation &right) { 
    	return left.v == right.v;
}
    friend bool operator!=(const TermRelation &left, const TermRelation &right) { return !(left == right); } 
  };
  
inline bool is_TermRelation_Stale(const struct TermRelation d);
  
inline bool is_TermRelation_Current(const struct TermRelation d);
  
inline bool is_TermRelation_New(const struct TermRelation d);
  
struct ReplicationState {
    uint64 nextIndex;
    uint64 matchIndex;
    uint64 heartBeatGen;
    ReplicationState(uint64 nextIndex, uint64 matchIndex, uint64 heartBeatGen) : nextIndex (nextIndex),  matchIndex (matchIndex),  heartBeatGen (heartBeatGen) {}
    ReplicationState();
    friend bool operator==(const ReplicationState &left, const ReplicationState &right) {
      	return true		&& left.nextIndex == right.nextIndex
      		&& left.matchIndex == right.matchIndex
      		&& left.heartBeatGen == right.heartBeatGen
      ;
    }
    friend bool operator!=(const ReplicationState &left, const ReplicationState &right) { return !(left == right); } 
  };
  
inline bool is_ReplicationState(const struct ReplicationState d) { (void) d; return true; }
}// end of namespace Raft  datatype declarations
namespace _module  {
}// end of namespace _module  datatype declarations
namespace Types  {
  class __default {
    public:
// Default constructor
    __default() {}
    static uint64 Min(uint64 a, uint64 b);static uint64 init__MaxUint64() {
      return (uint64)18446744073709551615U;
    }
    static uint64 MaxUint64;
  };
  class class_uint64 {
    public:
// Default constructor
    class_uint64() {}
    static uint64 get_Default() {
      return 0;
    }
  };
}// end of namespace Types  class declarations
namespace Raft  {
  class __default {
    public:
// Default constructor
    __default() {}
    template <typename __T>
    static std::shared_ptr<Raft::StateMachine <__T> > StateMachineFactory(uint64 id, uint64 f, uint64 total__num, __T sentinel);
static bool CandidateLogUpToDate(uint64 candidateLastLogTerm, uint64 localLastLogTerm, uint64 candidateLastLogIndex, uint64 localLastLogIndex);static Raft::TermRelation CompareTerms(uint64 lTerm, uint64 rTerm);static Raft::ReplicationState init__unusedIndex() {
      return Raft::ReplicationState((uint64)0, (uint64)0, (uint64)0);
    }
    static Raft::ReplicationState unusedIndex;
    static uint64 init__maxEntries() {
      return (uint64)5;
    }
    static uint64 maxEntries;
  };
  template <typename T>
  class StateMachine {
    public:
// Default constructor
    StateMachine() {}
    uint64 currentTerm = 0;
    uint64 votedFor = 0;
    DafnyArray<Types::Entry <T> > log = DafnyArray<Types::Entry <T> >::Null();
    DafnyArray<Raft::ReplicationState> replicationStates = DafnyArray<Raft::ReplicationState>::Null();
    uint64 lastLogIndex = 0;
    uint64 commitIndex = 0;
    uint64 lastApplied = 0;
    Raft::Role role = Raft::Role();
    DafnySequence<uint64> votes = DafnySequence<uint64>();
    uint64 timeOutGen = 0;
    
    Types::Entry <T>  SentinelEntry(T default_);
    void __ctor(uint64 id, uint64 f, uint64 total__num, T sentinel);

    Types::Output <T>  HandleEvent(Types::Event <T>  e);

    Types::Output <T>  OnRpc(Types::Message <T>  msg);

    Types::Output <T>  OnClientRequest(T req);

    Types::Output <T>  TryStartElection(uint64 gen);

    Types::Output <T>  TryReplicateToFollower(uint64 fid, uint64 gen);

    Types::Output <T>  OnAppendEntriesRpc(Types::Message <T>  ae);

    Types::Output <T>  OnAppendEntriesResponseRpc(Types::Message <T>  aer);

    Types::Output <T>  OnRequestVoteRpc(Types::Message <T>  rv);

    Types::Output <T>  OnRequestVoteResponseRpc(Types::Message <T>  rvr);

    uint64 LogLen();
    void TryAdvanceCommitIndex(uint64 maxIndex);

    uint64 CountReplicasWithMatchAtLeast(uint64 index);

    DafnyArray<Raft::ReplicationState> NewReplicationStates();
    DafnyArray<Types::Entry<T>> NewLog(T sentinel);

    DafnyArray<Raft::ReplicationState> NewLeaderReplicationStates(uint64 lli);

    void UpdateLog(Types::Message <T>  ae);

    void AppendNewEntries(Types::Message <T>  ae, uint64 startIdx);
uint64 _total__num = 0;
    uint64 init__total__num() {
      return this->total__num;
    }
    uint64 total__num =  init__total__num();
    uint64 init__logSize() {
      return (uint64)1048576;
    }
    uint64 logSize =  init__logSize();
    uint64 _id = 0;
    uint64 init__id() {
      return this->id;
    }
    uint64 id =  init__id();
    uint64 _quorum = 0;
    uint64 init__quorum() {
      return this->quorum;
    }
    uint64 quorum =  init__quorum();
  };
}// end of namespace Raft  class declarations
namespace _module  {
}// end of namespace _module  class declarations
template <typename T>
struct std::hash<Types::Entry<T>> {
  std::size_t operator()(const Types::Entry<T>& x) const {
    size_t seed = 0;
    hash_combine<uint64>(seed, x.term);
    hash_combine<T>(seed, x.command);
    return seed;
  }
};
template <typename T>
struct std::hash<Types::Message_AppendEntries<T>> {
  std::size_t operator()(const Types::Message_AppendEntries<T>& x) const {
    size_t seed = 0;
    hash_combine<uint64>(seed, x.term);
    hash_combine<uint64>(seed, x.leaderId);
    hash_combine<uint64>(seed, x.prevLogIndex);
    hash_combine<uint64>(seed, x.prevLogTerm);
    hash_combine<DafnySequence<Types::Entry <T> >>(seed, x.entries);
    hash_combine<uint64>(seed, x.leaderCommitIndex);
    return seed;
  }
};
template <typename T>
struct std::hash<Types::Message_AppendEntriesResponse<T>> {
  std::size_t operator()(const Types::Message_AppendEntriesResponse<T>& x) const {
    size_t seed = 0;
    hash_combine<uint64>(seed, x.term);
    hash_combine<bool>(seed, x.success);
    hash_combine<uint64>(seed, x.senderId);
    hash_combine<uint64>(seed, x.lastLogIndex);
    return seed;
  }
};
template <typename T>
struct std::hash<Types::Message_RequestVote<T>> {
  std::size_t operator()(const Types::Message_RequestVote<T>& x) const {
    size_t seed = 0;
    hash_combine<uint64>(seed, x.term);
    hash_combine<uint64>(seed, x.candidateId);
    hash_combine<uint64>(seed, x.lastLogIndex);
    hash_combine<uint64>(seed, x.lastLogTerm);
    return seed;
  }
};
template <typename T>
struct std::hash<Types::Message_RequestVoteResponse<T>> {
  std::size_t operator()(const Types::Message_RequestVoteResponse<T>& x) const {
    size_t seed = 0;
    hash_combine<uint64>(seed, x.term);
    hash_combine<bool>(seed, x.voteGranted);
    hash_combine<uint64>(seed, x.senderId);
    return seed;
  }
};
template <typename T>
struct std::hash<Types::Message<T>> {
  std::size_t operator()(const Types::Message<T>& x) const {
    size_t seed = 0;
    if (x.is_Message_AppendEntries()) {
      hash_combine<uint64>(seed, 0);
      hash_combine<struct Types::Message_AppendEntries<T>>(seed, std::get<Types::Message_AppendEntries<T>>(x.v));
    }
    if (x.is_Message_AppendEntriesResponse()) {
      hash_combine<uint64>(seed, 1);
      hash_combine<struct Types::Message_AppendEntriesResponse<T>>(seed, std::get<Types::Message_AppendEntriesResponse<T>>(x.v));
    }
    if (x.is_Message_RequestVote()) {
      hash_combine<uint64>(seed, 2);
      hash_combine<struct Types::Message_RequestVote<T>>(seed, std::get<Types::Message_RequestVote<T>>(x.v));
    }
    if (x.is_Message_RequestVoteResponse()) {
      hash_combine<uint64>(seed, 3);
      hash_combine<struct Types::Message_RequestVoteResponse<T>>(seed, std::get<Types::Message_RequestVoteResponse<T>>(x.v));
    }
    return seed;
  }
};
template <typename T>
struct std::hash<Types::Event_ReceiveTransaction<T>> {
  std::size_t operator()(const Types::Event_ReceiveTransaction<T>& x) const {
    size_t seed = 0;
    hash_combine<T>(seed, x.request);
    return seed;
  }
};
template <typename T>
struct std::hash<Types::Event_ReceiveMessage<T>> {
  std::size_t operator()(const Types::Event_ReceiveMessage<T>& x) const {
    size_t seed = 0;
    hash_combine<Types::Message <T> >(seed, x.message);
    return seed;
  }
};
template <typename T>
struct std::hash<Types::Event_PossibleTimeOut<T>> {
  std::size_t operator()(const Types::Event_PossibleTimeOut<T>& x) const {
    size_t seed = 0;
    hash_combine<uint64>(seed, x.timeOutGenSnapshot);
    return seed;
  }
};
template <typename T>
struct std::hash<Types::Event_PossibleHeartBeatNeeded<T>> {
  std::size_t operator()(const Types::Event_PossibleHeartBeatNeeded<T>& x) const {
    size_t seed = 0;
    hash_combine<uint64>(seed, x.followerId);
    hash_combine<uint64>(seed, x.heartBeatGenSnapshot);
    return seed;
  }
};
template <typename T>
struct std::hash<Types::Event<T>> {
  std::size_t operator()(const Types::Event<T>& x) const {
    size_t seed = 0;
    if (x.is_Event_ReceiveTransaction()) {
      hash_combine<uint64>(seed, 0);
      hash_combine<struct Types::Event_ReceiveTransaction<T>>(seed, std::get<Types::Event_ReceiveTransaction<T>>(x.v));
    }
    if (x.is_Event_ReceiveMessage()) {
      hash_combine<uint64>(seed, 1);
      hash_combine<struct Types::Event_ReceiveMessage<T>>(seed, std::get<Types::Event_ReceiveMessage<T>>(x.v));
    }
    if (x.is_Event_PossibleTimeOut()) {
      hash_combine<uint64>(seed, 2);
      hash_combine<struct Types::Event_PossibleTimeOut<T>>(seed, std::get<Types::Event_PossibleTimeOut<T>>(x.v));
    }
    if (x.is_Event_PossibleHeartBeatNeeded()) {
      hash_combine<uint64>(seed, 3);
      hash_combine<struct Types::Event_PossibleHeartBeatNeeded<T>>(seed, std::get<Types::Event_PossibleHeartBeatNeeded<T>>(x.v));
    }
    return seed;
  }
};
template <typename T>
struct std::hash<Types::Send<T>> {
  std::size_t operator()(const Types::Send<T>& x) const {
    size_t seed = 0;
    hash_combine<uint64>(seed, x.to);
    hash_combine<Types::Message <T> >(seed, x.msg);
    return seed;
  }
};
template <typename T>
struct std::hash<Types::MessageOutput_Messages<T>> {
  std::size_t operator()(const Types::MessageOutput_Messages<T>& x) const {
    size_t seed = 0;
    hash_combine<DafnySequence<Types::Send <T> >>(seed, x.msgs);
    return seed;
  }
};
template <typename T>
struct std::hash<Types::MessageOutput_NoMessage<T>> {
  std::size_t operator()(const Types::MessageOutput_NoMessage<T>& x) const {
    size_t seed = 0;
    (void)x;
    return seed;
  }
};
template <typename T>
struct std::hash<Types::MessageOutput<T>> {
  std::size_t operator()(const Types::MessageOutput<T>& x) const {
    size_t seed = 0;
    if (x.is_MessageOutput_Messages()) {
      hash_combine<uint64>(seed, 0);
      hash_combine<struct Types::MessageOutput_Messages<T>>(seed, std::get<Types::MessageOutput_Messages<T>>(x.v));
    }
    if (x.is_MessageOutput_NoMessage()) {
      hash_combine<uint64>(seed, 1);
      hash_combine<struct Types::MessageOutput_NoMessage<T>>(seed, std::get<Types::MessageOutput_NoMessage<T>>(x.v));
    }
    return seed;
  }
};
template <>
struct std::hash<Types::TimerCommand_SetElectionTimer> {
  std::size_t operator()(const Types::TimerCommand_SetElectionTimer& x) const {
    size_t seed = 0;
    hash_combine<uint64>(seed, x.gen);
    return seed;
  }
};
template <>
struct std::hash<Types::TimerCommand_SetReplicateTimer> {
  std::size_t operator()(const Types::TimerCommand_SetReplicateTimer& x) const {
    size_t seed = 0;
    hash_combine<uint64>(seed, x.followerId);
    hash_combine<uint64>(seed, x.gen);
    return seed;
  }
};
template <>
struct std::hash<Types::TimerCommand> {
  std::size_t operator()(const Types::TimerCommand& x) const {
    size_t seed = 0;
    if (x.is_TimerCommand_SetElectionTimer()) {
      hash_combine<uint64>(seed, 0);
      hash_combine<struct Types::TimerCommand_SetElectionTimer>(seed, std::get<Types::TimerCommand_SetElectionTimer>(x.v));
    }
    if (x.is_TimerCommand_SetReplicateTimer()) {
      hash_combine<uint64>(seed, 1);
      hash_combine<struct Types::TimerCommand_SetReplicateTimer>(seed, std::get<Types::TimerCommand_SetReplicateTimer>(x.v));
    }
    return seed;
  }
};
template <>
struct std::hash<Types::TimerOutput_TimerChanges> {
  std::size_t operator()(const Types::TimerOutput_TimerChanges& x) const {
    size_t seed = 0;
    hash_combine<DafnySequence<Types::TimerCommand>>(seed, x.changes);
    return seed;
  }
};
template <>
struct std::hash<Types::TimerOutput_NoTimerChange> {
  std::size_t operator()(const Types::TimerOutput_NoTimerChange& x) const {
    size_t seed = 0;
    (void)x;
    return seed;
  }
};
template <>
struct std::hash<Types::TimerOutput> {
  std::size_t operator()(const Types::TimerOutput& x) const {
    size_t seed = 0;
    if (x.is_TimerOutput_TimerChanges()) {
      hash_combine<uint64>(seed, 0);
      hash_combine<struct Types::TimerOutput_TimerChanges>(seed, std::get<Types::TimerOutput_TimerChanges>(x.v));
    }
    if (x.is_TimerOutput_NoTimerChange()) {
      hash_combine<uint64>(seed, 1);
      hash_combine<struct Types::TimerOutput_NoTimerChange>(seed, std::get<Types::TimerOutput_NoTimerChange>(x.v));
    }
    return seed;
  }
};
template <>
struct std::hash<Types::CommitOutput_CommitAdvanced> {
  std::size_t operator()(const Types::CommitOutput_CommitAdvanced& x) const {
    size_t seed = 0;
    hash_combine<uint64>(seed, x.lastApplied);
    hash_combine<uint64>(seed, x.commitIndex);
    return seed;
  }
};
template <>
struct std::hash<Types::CommitOutput_NoCommit> {
  std::size_t operator()(const Types::CommitOutput_NoCommit& x) const {
    size_t seed = 0;
    (void)x;
    return seed;
  }
};
template <>
struct std::hash<Types::CommitOutput> {
  std::size_t operator()(const Types::CommitOutput& x) const {
    size_t seed = 0;
    if (x.is_CommitOutput_CommitAdvanced()) {
      hash_combine<uint64>(seed, 0);
      hash_combine<struct Types::CommitOutput_CommitAdvanced>(seed, std::get<Types::CommitOutput_CommitAdvanced>(x.v));
    }
    if (x.is_CommitOutput_NoCommit()) {
      hash_combine<uint64>(seed, 1);
      hash_combine<struct Types::CommitOutput_NoCommit>(seed, std::get<Types::CommitOutput_NoCommit>(x.v));
    }
    return seed;
  }
};
template <typename T>
struct std::hash<Types::Output<T>> {
  std::size_t operator()(const Types::Output<T>& x) const {
    size_t seed = 0;
    hash_combine<Types::MessageOutput <T> >(seed, x.messages);
    hash_combine<Types::TimerOutput>(seed, x.timer);
    hash_combine<Types::CommitOutput>(seed, x.commit);
    return seed;
  }
};
template <>
struct std::hash<Raft::Role_Follower> {
  std::size_t operator()(const Raft::Role_Follower& x) const {
    size_t seed = 0;
    (void)x;
    return seed;
  }
};
template <>
struct std::hash<Raft::Role_Candidate> {
  std::size_t operator()(const Raft::Role_Candidate& x) const {
    size_t seed = 0;
    (void)x;
    return seed;
  }
};
template <>
struct std::hash<Raft::Role_Leader> {
  std::size_t operator()(const Raft::Role_Leader& x) const {
    size_t seed = 0;
    (void)x;
    return seed;
  }
};
template <>
struct std::hash<Raft::Role> {
  std::size_t operator()(const Raft::Role& x) const {
    size_t seed = 0;
    if (x.is_Role_Follower()) {
      hash_combine<uint64>(seed, 0);
      hash_combine<struct Raft::Role_Follower>(seed, std::get<Raft::Role_Follower>(x.v));
    }
    if (x.is_Role_Candidate()) {
      hash_combine<uint64>(seed, 1);
      hash_combine<struct Raft::Role_Candidate>(seed, std::get<Raft::Role_Candidate>(x.v));
    }
    if (x.is_Role_Leader()) {
      hash_combine<uint64>(seed, 2);
      hash_combine<struct Raft::Role_Leader>(seed, std::get<Raft::Role_Leader>(x.v));
    }
    return seed;
  }
};
template <>
struct std::hash<Raft::TermRelation_Stale> {
  std::size_t operator()(const Raft::TermRelation_Stale& x) const {
    size_t seed = 0;
    (void)x;
    return seed;
  }
};
template <>
struct std::hash<Raft::TermRelation_Current> {
  std::size_t operator()(const Raft::TermRelation_Current& x) const {
    size_t seed = 0;
    (void)x;
    return seed;
  }
};
template <>
struct std::hash<Raft::TermRelation_New> {
  std::size_t operator()(const Raft::TermRelation_New& x) const {
    size_t seed = 0;
    (void)x;
    return seed;
  }
};
template <>
struct std::hash<Raft::TermRelation> {
  std::size_t operator()(const Raft::TermRelation& x) const {
    size_t seed = 0;
    if (x.is_TermRelation_Stale()) {
      hash_combine<uint64>(seed, 0);
      hash_combine<struct Raft::TermRelation_Stale>(seed, std::get<Raft::TermRelation_Stale>(x.v));
    }
    if (x.is_TermRelation_Current()) {
      hash_combine<uint64>(seed, 1);
      hash_combine<struct Raft::TermRelation_Current>(seed, std::get<Raft::TermRelation_Current>(x.v));
    }
    if (x.is_TermRelation_New()) {
      hash_combine<uint64>(seed, 2);
      hash_combine<struct Raft::TermRelation_New>(seed, std::get<Raft::TermRelation_New>(x.v));
    }
    return seed;
  }
};
template <>
struct std::hash<Raft::ReplicationState> {
  std::size_t operator()(const Raft::ReplicationState& x) const {
    size_t seed = 0;
    hash_combine<uint64>(seed, x.nextIndex);
    hash_combine<uint64>(seed, x.matchIndex);
    hash_combine<uint64>(seed, x.heartBeatGen);
    return seed;
  }
};
