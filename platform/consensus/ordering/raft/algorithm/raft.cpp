// Dafny program the_program compiled into Cpp
#include "DafnyRuntime.h"
using namespace std::literals;
#include "the_pro.h"
namespace Types  {

  uint64 __default::Min(uint64 a, uint64 b)
  {
    if ((a) < (b)) {
      return a;
    } else {
      return b;
    }
  }
   uint64 __default::MaxUint64 =  init__MaxUint64();

  typedef uint64 uint64;

  template <typename T>
Entry<T>::Entry() {
    term = 0;
    command = get_default<T>::call();
  }

  template <typename T>
Message<T>::Message() {
    Message_AppendEntries<T> COMPILER_result_subStruct;
    COMPILER_result_subStruct.term = 0;
    COMPILER_result_subStruct.leaderId = 0;
    COMPILER_result_subStruct.prevLogIndex = 0;
    COMPILER_result_subStruct.prevLogTerm = 0;
    COMPILER_result_subStruct.entries = DafnySequence<Types::Entry <T> >();
    COMPILER_result_subStruct.leaderCommitIndex = 0;
    v = COMPILER_result_subStruct;
  }
  template <typename T>
inline bool is_Message_AppendEntries(const struct Message<T> d) { return std::holds_alternative<Message_AppendEntries<T>>(d.v); }
  template <typename T>
inline bool is_Message_AppendEntriesResponse(const struct Message<T> d) { return std::holds_alternative<Message_AppendEntriesResponse<T>>(d.v); }
  template <typename T>
inline bool is_Message_RequestVote(const struct Message<T> d) { return std::holds_alternative<Message_RequestVote<T>>(d.v); }
  template <typename T>
inline bool is_Message_RequestVoteResponse(const struct Message<T> d) { return std::holds_alternative<Message_RequestVoteResponse<T>>(d.v); }

  template <typename T>
Event<T>::Event() {
    Event_ReceiveTransaction<T> COMPILER_result_subStruct;
    COMPILER_result_subStruct.request = get_default<T>::call();
    v = COMPILER_result_subStruct;
  }
  template <typename T>
inline bool is_Event_ReceiveTransaction(const struct Event<T> d) { return std::holds_alternative<Event_ReceiveTransaction<T>>(d.v); }
  template <typename T>
inline bool is_Event_ReceiveMessage(const struct Event<T> d) { return std::holds_alternative<Event_ReceiveMessage<T>>(d.v); }
  template <typename T>
inline bool is_Event_PossibleTimeOut(const struct Event<T> d) { return std::holds_alternative<Event_PossibleTimeOut<T>>(d.v); }
  template <typename T>
inline bool is_Event_PossibleHeartBeatNeeded(const struct Event<T> d) { return std::holds_alternative<Event_PossibleHeartBeatNeeded<T>>(d.v); }

  template <typename T>
Send<T>::Send() {
    to = 0;
    msg = Types::Message<T>();
  }

  template <typename T>
MessageOutput<T>::MessageOutput() {
    MessageOutput_Messages<T> COMPILER_result_subStruct;
    COMPILER_result_subStruct.msgs = DafnySequence<Types::Send <T> >();
    v = COMPILER_result_subStruct;
  }
  template <typename T>
inline bool is_MessageOutput_Messages(const struct MessageOutput<T> d) { return std::holds_alternative<MessageOutput_Messages<T>>(d.v); }
  template <typename T>
inline bool is_MessageOutput_NoMessage(const struct MessageOutput<T> d) { return std::holds_alternative<MessageOutput_NoMessage<T>>(d.v); }

  
TimerCommand::TimerCommand() {
    TimerCommand_SetElectionTimer COMPILER_result_subStruct;
    COMPILER_result_subStruct.gen = 0;
    v = COMPILER_result_subStruct;
  }
  
inline bool is_TimerCommand_SetElectionTimer(const struct TimerCommand d) { return std::holds_alternative<TimerCommand_SetElectionTimer>(d.v); }
  
inline bool is_TimerCommand_SetReplicateTimer(const struct TimerCommand d) { return std::holds_alternative<TimerCommand_SetReplicateTimer>(d.v); }

  
TimerOutput::TimerOutput() {
    TimerOutput_TimerChanges COMPILER_result_subStruct;
    COMPILER_result_subStruct.changes = DafnySequence<Types::TimerCommand>();
    v = COMPILER_result_subStruct;
  }
  
inline bool is_TimerOutput_TimerChanges(const struct TimerOutput d) { return std::holds_alternative<TimerOutput_TimerChanges>(d.v); }
  
inline bool is_TimerOutput_NoTimerChange(const struct TimerOutput d) { return std::holds_alternative<TimerOutput_NoTimerChange>(d.v); }

  
CommitOutput::CommitOutput() {
    CommitOutput_CommitAdvanced COMPILER_result_subStruct;
    COMPILER_result_subStruct.lastApplied = 0;
    COMPILER_result_subStruct.commitIndex = 0;
    v = COMPILER_result_subStruct;
  }
  
inline bool is_CommitOutput_CommitAdvanced(const struct CommitOutput d) { return std::holds_alternative<CommitOutput_CommitAdvanced>(d.v); }
  
inline bool is_CommitOutput_NoCommit(const struct CommitOutput d) { return std::holds_alternative<CommitOutput_NoCommit>(d.v); }

  template <typename T>
Output<T>::Output() {
    messages = Types::MessageOutput<T>();
    timer = Types::TimerOutput();
    commit = Types::CommitOutput();
  }
}// end of namespace Types 
namespace Raft  {

  template <typename __T>
  std::shared_ptr<Raft::StateMachine <__T> > __default::StateMachineFactory(uint64 id, uint64 f, uint64 total__num, __T sentinel)
  {
    std::shared_ptr<Raft::StateMachine <__T> > r = nullptr;
    std::shared_ptr<Raft::StateMachine <__T> > _nw0 = std::make_shared<Raft::StateMachine <__T> > ();
    _nw0->__ctor(id, f, total__num, sentinel);
    r = _nw0;
    return r;
  }
  bool __default::CandidateLogUpToDate(uint64 candidateLastLogTerm, uint64 localLastLogTerm, uint64 candidateLastLogIndex, uint64 localLastLogIndex)
  {
    return ((candidateLastLogTerm) > (localLastLogTerm)) || (((candidateLastLogTerm) == (localLastLogTerm)) && ((candidateLastLogIndex) >= (localLastLogIndex)));
  }
  Raft::TermRelation __default::CompareTerms(uint64 lTerm, uint64 rTerm)
  {
    if ((lTerm) > (rTerm)) {
      return Raft::TermRelation::create_Stale();
    } else if ((lTerm) == (rTerm)) {
      return Raft::TermRelation::create_Current();
    } else {
      return Raft::TermRelation::create_New();
    }
  }
   Raft::ReplicationState __default::unusedIndex =  init__unusedIndex();
   uint64 __default::maxEntries =  init__maxEntries();

  
Role::Role() {
    Role_Follower COMPILER_result_subStruct;
    v = COMPILER_result_subStruct;
  }
  
inline bool is_Role_Follower(const struct Role d) { return std::holds_alternative<Role_Follower>(d.v); }
  
inline bool is_Role_Candidate(const struct Role d) { return std::holds_alternative<Role_Candidate>(d.v); }
  
inline bool is_Role_Leader(const struct Role d) { return std::holds_alternative<Role_Leader>(d.v); }

  
TermRelation::TermRelation() {
    TermRelation_Stale COMPILER_result_subStruct;
    v = COMPILER_result_subStruct;
  }
  
inline bool is_TermRelation_Stale(const struct TermRelation d) { return std::holds_alternative<TermRelation_Stale>(d.v); }
  
inline bool is_TermRelation_Current(const struct TermRelation d) { return std::holds_alternative<TermRelation_Current>(d.v); }
  
inline bool is_TermRelation_New(const struct TermRelation d) { return std::holds_alternative<TermRelation_New>(d.v); }

  
ReplicationState::ReplicationState() {
    nextIndex = 0;
    matchIndex = 0;
    heartBeatGen = 0;
  }

  
  template <typename T>
  Types::Entry <T>  StateMachine<T>::SentinelEntry(T default_)
  {
    return Types::Entry<T>((uint64)0, default_);
  }
  
  template <typename T>
  void StateMachine<T>::__ctor(uint64 id, uint64 f, uint64 total__num, T sentinel)
  {
    (this)->quorum = ((total__num) / ((uint64)2)) + ((uint64)1);
    (this)->id = id;
    (this)->total__num = total__num;
    (this)->role = Raft::Role::create_Follower();
    (this)->currentTerm = (uint64)0;
    (this)->votedFor = (uint64)0;
    (this)->lastLogIndex = (uint64)0;
    (this)->commitIndex = (uint64)0;
    (this)->lastApplied = (uint64)0;
    (this)->timeOutGen = (uint64)0;
    (this)->votes = DafnySequence<uint64>::Create({});
    DafnyArray<Types::Entry <T> > _out0;
    _out0 = (this)->NewLog(sentinel);
    (this)->log = _out0;
    DafnyArray<Raft::ReplicationState> _out1;
    _out1 = (this)->NewReplicationStates();
    (this)->replicationStates = _out1;
  }
  
  template <typename T>
  Types::Output <T>  StateMachine<T>::HandleEvent(Types::Event <T>  e)
  {
    Types::Output <T>  out = Types::Output<T>();
    Types::Event <T>  _source0 = e;
    {
      if (is_Event_ReceiveTransaction(_source0)) {
        T _0_req = ((_source0).dtor_request());
        Types::Output <T>  _out0;
        _out0 = (this)->OnClientRequest(_0_req);
        out = _out0;
        goto after_match0;
      }
    }
    {
      if (is_Event_ReceiveMessage(_source0)) {
        Types::Message <T>  _1_msg = ((_source0).dtor_message());
        Types::Output <T>  _out1;
        _out1 = (this)->OnRpc(_1_msg);
        out = _out1;
        goto after_match0;
      }
    }
    {
      if (is_Event_PossibleTimeOut(_source0)) {
        uint64 _2_gen = ((_source0).dtor_timeOutGenSnapshot());
        Types::Output <T>  _out2;
        _out2 = (this)->TryStartElection(_2_gen);
        out = _out2;
        goto after_match0;
      }
    }
    {
      uint64 _3_fid = ((_source0).dtor_followerId());
      uint64 _4_gen = ((_source0).dtor_heartBeatGenSnapshot());
      Types::Output <T>  _out3;
      _out3 = (this)->TryReplicateToFollower(_3_fid, _4_gen);
      out = _out3;
    }
  after_match0: ;
    return out;
  }
  
  template <typename T>
  Types::Output <T>  StateMachine<T>::OnRpc(Types::Message <T>  msg)
  {
    Types::Output <T>  out = Types::Output<T>();
    Types::Message <T>  _source0 = msg;
    {
      if (is_Message_AppendEntries(_source0)) {
        Types::Output <T>  _out0;
        _out0 = (this)->OnAppendEntriesRpc(msg);
        out = _out0;
        goto after_match0;
      }
    }
    {
      if (is_Message_AppendEntriesResponse(_source0)) {
        Types::Output <T>  _out1;
        _out1 = (this)->OnAppendEntriesResponseRpc(msg);
        out = _out1;
        goto after_match0;
      }
    }
    {
      if (is_Message_RequestVote(_source0)) {
        Types::Output <T>  _out2;
        _out2 = (this)->OnRequestVoteRpc(msg);
        out = _out2;
        goto after_match0;
      }
    }
    {
      Types::Output <T>  _out3;
      _out3 = (this)->OnRequestVoteResponseRpc(msg);
      out = _out3;
    }
  after_match0: ;
    return out;
  }
  
  template <typename T>
  Types::Output <T>  StateMachine<T>::OnClientRequest(T req)
  {
    Types::Output <T>  out = Types::Output<T>();
    out = Types::Output<T>(Types::MessageOutput <T> ::create_NoMessage(), Types::TimerOutput::create_NoTimerChange(), Types::CommitOutput::create_NoCommit());
    if ((this->role) != (Raft::Role::create_Leader())) {
      return out;
    }
    Types::Entry <T>  _0_entry;
    _0_entry = Types::Entry<T>(this->currentTerm, req);
    DafnyArray<Types::Entry <T> > _arr0 = this->log;
    uint64 _index0 = (this->lastLogIndex) + ((uint64)1);
    _arr0.at(_index0) = _0_entry;
    (this)->lastLogIndex = (this->lastLogIndex) + ((uint64)1);
    DafnyArray<Raft::ReplicationState> _arr1 = this->replicationStates;
    _arr1.at(((this)->id)) = Raft::ReplicationState((this->lastLogIndex) + ((uint64)1), this->lastLogIndex, (((this->replicationStates).at((this)->id)).heartBeatGen));
    return out;
  }
  
  template <typename T>
  Types::Output <T>  StateMachine<T>::TryStartElection(uint64 gen)
  {
    Types::Output <T>  out = Types::Output<T>();
    if (((gen) != (this->timeOutGen)) || ((this->role) == (Raft::Role::create_Leader()))) {
      out = Types::Output<T>(Types::MessageOutput <T> ::create_NoMessage(), Types::TimerOutput::create_NoTimerChange(), Types::CommitOutput::create_NoCommit());
      return out;
    }
    (this)->role = Raft::Role::create_Candidate();
    (this)->currentTerm = (this->currentTerm) + ((uint64)1);
    (this)->votedFor = (this)->id;
    (this)->votes = DafnySequence<uint64>::Create({(this)->id});
    (this)->timeOutGen = (this->timeOutGen) + ((uint64)1);
    Types::Message <T>  _0_requestVoteMsg;
    _0_requestVoteMsg = Types::Message <T> ::create_RequestVote(this->currentTerm, (this)->id, this->lastLogIndex, (((this->log).at(this->lastLogIndex)).term));
    DafnySequence<Types::Send <T> > _1_sends;
    _1_sends = DafnySequence<Types::Send <T> >::Create({});
    uint64 _2_i;
    _2_i = (uint64)1;
    while ((_2_i) < (((this)->total__num) + ((uint64)1))) {
      if ((_2_i) != ((this)->id)) {
        _1_sends = (_1_sends).concatenate(DafnySequence<Types::Send <T> >::Create({Types::Send<T>(_2_i, _0_requestVoteMsg)}));
      }
      _2_i = (_2_i) + ((uint64)1);
    }
    out = Types::Output<T>(Types::MessageOutput <T> ::create_Messages(_1_sends), Types::TimerOutput::create_TimerChanges(DafnySequence<Types::TimerCommand>::Create({Types::TimerCommand::create_SetElectionTimer(this->timeOutGen)})), Types::CommitOutput::create_NoCommit());
    return out;
  }
  
  template <typename T>
  Types::Output <T>  StateMachine<T>::TryReplicateToFollower(uint64 fid, uint64 gen)
  {
    Types::Output <T>  out = Types::Output<T>();
    if ((((this->role) != (Raft::Role::create_Leader())) || ((fid) == ((this)->id))) || ((gen) != ((((this->replicationStates).at(fid)).heartBeatGen)))) {
      out = Types::Output<T>(Types::MessageOutput <T> ::create_NoMessage(), Types::TimerOutput::create_NoTimerChange(), Types::CommitOutput::create_NoCommit());
      return out;
    }
    uint64 _0_nextIndex;
    _0_nextIndex = (((this->replicationStates).at(fid)).nextIndex);
    DafnySequence<Types::Entry <T> > _1_entries;
    _1_entries = DafnySequence<Types::Entry <T> >::Create({});
    if ((_0_nextIndex) <= (this->lastLogIndex)) {
      _1_entries = DafnySequence<Types::Entry <T> >::SeqFromArraySlice((this->log),(_0_nextIndex),(Types::__default::Min((_0_nextIndex) + (Raft::__default::maxEntries), (this->lastLogIndex) + ((uint64)1))));
    }
    Types::MessageOutput <T>  _2_sends;
    _2_sends = Types::MessageOutput <T> ::create_Messages(DafnySequence<Types::Send <T> >::Create({Types::Send<T>(fid, Types::Message <T> ::create_AppendEntries(this->currentTerm, (this)->id, (_0_nextIndex) - ((uint64)1), (((this->log).at((_0_nextIndex) - ((uint64)1))).term), _1_entries, this->commitIndex))}));
    DafnyArray<Raft::ReplicationState> _arr0 = this->replicationStates;
    _arr0.at((fid)) = Raft::ReplicationState((((this->replicationStates).at(fid)).nextIndex), (((this->replicationStates).at(fid)).matchIndex), ((((this->replicationStates).at(fid)).heartBeatGen)) + ((uint64)1));
    Types::TimerOutput _3_timer;
    _3_timer = Types::TimerOutput::create_TimerChanges(DafnySequence<Types::TimerCommand>::Create({Types::TimerCommand::create_SetReplicateTimer(fid, (((this->replicationStates).at(fid)).heartBeatGen))}));
    out = Types::Output<T>(_2_sends, _3_timer, Types::CommitOutput::create_NoCommit());
    return out;
  }
  
  template <typename T>
  Types::Output <T>  StateMachine<T>::OnAppendEntriesRpc(Types::Message <T>  ae)
  {
    Types::Output <T>  out = Types::Output<T>();
    Raft::TermRelation _0_msgTermIs;
    _0_msgTermIs = Raft::__default::CompareTerms(this->currentTerm, ((ae).dtor_term()));
    if ((_0_msgTermIs) == (Raft::TermRelation::create_Stale())) {
      out = Types::Output<T>(Types::MessageOutput <T> ::create_NoMessage(), Types::TimerOutput::create_NoTimerChange(), Types::CommitOutput::create_NoCommit());
      return out;
    }
    if ((_0_msgTermIs) == (Raft::TermRelation::create_New())) {
      (this)->currentTerm = ((ae).dtor_term());
      (this)->votedFor = (uint64)0;
      (this)->votes = DafnySequence<uint64>::Create({});
      (this)->role = Raft::Role::create_Follower();
      DafnyArray<Raft::ReplicationState> _out0;
      _out0 = (this)->NewReplicationStates();
      (this)->replicationStates = _out0;
    }
    if ((this->role) != (Raft::Role::create_Follower())) {
      (this)->role = Raft::Role::create_Follower();
      (this)->votes = DafnySequence<uint64>::Create({});
      DafnyArray<Raft::ReplicationState> _out1;
      _out1 = (this)->NewReplicationStates();
      (this)->replicationStates = _out1;
    }
    (this)->timeOutGen = (this->timeOutGen) + ((uint64)1);
    Types::TimerOutput _1_timer;
    _1_timer = Types::TimerOutput::create_TimerChanges(DafnySequence<Types::TimerCommand>::Create({Types::TimerCommand::create_SetElectionTimer(this->timeOutGen)}));
    bool _2_success;
    _2_success = (((ae).dtor_prevLogTerm())) == ((((this->log).at(((ae).dtor_prevLogIndex()))).term));
    if (!(_2_success)) {
      Types::MessageOutput <T>  _3_aer;
      _3_aer = Types::MessageOutput <T> ::create_Messages(DafnySequence<Types::Send <T> >::Create({Types::Send<T>(((ae).dtor_leaderId()), Types::Message <T> ::create_AppendEntriesResponse(this->currentTerm, _2_success, (this)->id, this->lastLogIndex))}));
      out = Types::Output<T>(_3_aer, _1_timer, Types::CommitOutput::create_NoCommit());
      return out;
    }
    (this)->UpdateLog(ae);
    Types::CommitOutput _4_commits;
    _4_commits = Types::CommitOutput::create_NoCommit();
    if ((((ae).dtor_leaderCommitIndex())) > (this->commitIndex)) {
      (this)->commitIndex = Types::__default::Min(this->lastLogIndex, ((ae).dtor_leaderCommitIndex()));
      if ((this->commitIndex) > (this->lastApplied)) {
        _4_commits = Types::CommitOutput::create_CommitAdvanced((this->lastApplied) + ((uint64)1), this->commitIndex);
        (this)->lastApplied = this->commitIndex;
      }
    }
    Types::MessageOutput <T>  _5_sends;
    _5_sends = Types::MessageOutput <T> ::create_Messages(DafnySequence<Types::Send <T> >::Create({Types::Send<T>(((ae).dtor_leaderId()), Types::Message <T> ::create_AppendEntriesResponse(this->currentTerm, _2_success, (this)->id, this->lastLogIndex))}));
    out = Types::Output<T>(_5_sends, _1_timer, _4_commits);
    return out;
  }
  
  template <typename T>
  Types::Output <T>  StateMachine<T>::OnAppendEntriesResponseRpc(Types::Message <T>  aer)
  {
    Types::Output <T>  out = Types::Output<T>();
    Raft::TermRelation _0_msgTermIs;
    _0_msgTermIs = Raft::__default::CompareTerms(this->currentTerm, ((aer).dtor_term()));
    if ((_0_msgTermIs) == (Raft::TermRelation::create_Stale())) {
      out = Types::Output<T>(Types::MessageOutput <T> ::create_NoMessage(), Types::TimerOutput::create_NoTimerChange(), Types::CommitOutput::create_NoCommit());
      return out;
    }
    if ((_0_msgTermIs) == (Raft::TermRelation::create_New())) {
      (this)->currentTerm = ((aer).dtor_term());
      (this)->votedFor = (uint64)0;
      (this)->votes = DafnySequence<uint64>::Create({});
      (this)->role = Raft::Role::create_Follower();
      DafnyArray<Raft::ReplicationState> _out0;
      _out0 = (this)->NewReplicationStates();
      (this)->replicationStates = _out0;
      (this)->timeOutGen = (this->timeOutGen) + ((uint64)1);
      out = Types::Output<T>(Types::MessageOutput <T> ::create_NoMessage(), Types::TimerOutput::create_TimerChanges(DafnySequence<Types::TimerCommand>::Create({Types::TimerCommand::create_SetElectionTimer(this->timeOutGen)})), Types::CommitOutput::create_NoCommit());
      return out;
    }
    if ((this->role) != (Raft::Role::create_Leader())) {
      out = Types::Output<T>(Types::MessageOutput <T> ::create_NoMessage(), Types::TimerOutput::create_NoTimerChange(), Types::CommitOutput::create_NoCommit());
      return out;
    }
    DafnyArray<Raft::ReplicationState> _arr0 = this->replicationStates;
    uint64 _index0 = ((aer).dtor_senderId());
    _arr0.at(_index0) = Raft::ReplicationState((((aer).dtor_lastLogIndex())) + ((uint64)1), (((this->replicationStates).at(((aer).dtor_senderId()))).matchIndex), (((this->replicationStates).at(((aer).dtor_senderId()))).heartBeatGen));
    Types::CommitOutput _1_commits;
    _1_commits = Types::CommitOutput::create_NoCommit();
    if (((aer).dtor_success())) {
      DafnyArray<Raft::ReplicationState> _arr1 = this->replicationStates;
      uint64 _index1 = ((aer).dtor_senderId());
      _arr1.at(_index1) = Raft::ReplicationState((((this->replicationStates).at(((aer).dtor_senderId()))).nextIndex), ((aer).dtor_lastLogIndex()), (((this->replicationStates).at(((aer).dtor_senderId()))).heartBeatGen));
      if ((((aer).dtor_lastLogIndex())) > (this->commitIndex)) {
        (this)->TryAdvanceCommitIndex(((aer).dtor_lastLogIndex()));
        if ((this->lastApplied) < (this->commitIndex)) {
          _1_commits = Types::CommitOutput::create_CommitAdvanced((this->lastApplied) + ((uint64)1), this->commitIndex);
          (this)->lastApplied = this->commitIndex;
        }
      }
    }
    if (((((this->replicationStates).at(((aer).dtor_senderId()))).nextIndex)) < ((this->lastLogIndex) + ((uint64)1))) {
      uint64 _2_nextIndex;
      _2_nextIndex = (((this->replicationStates).at(((aer).dtor_senderId()))).nextIndex);
      Types::MessageOutput <T>  _3_sends;
      _3_sends = Types::MessageOutput <T> ::create_Messages(DafnySequence<Types::Send <T> >::Create({Types::Send<T>(((aer).dtor_senderId()), Types::Message <T> ::create_AppendEntries(this->currentTerm, (this)->id, (_2_nextIndex) - ((uint64)1), (((this->log).at((_2_nextIndex) - ((uint64)1))).term), DafnySequence<Types::Entry <T> >::SeqFromArraySlice((this->log),(_2_nextIndex),(Types::__default::Min((_2_nextIndex) + (Raft::__default::maxEntries), (this->lastLogIndex) + ((uint64)1)))), this->commitIndex))}));
      DafnyArray<Raft::ReplicationState> _arr2 = this->replicationStates;
      uint64 _index2 = ((aer).dtor_senderId());
      _arr2.at(_index2) = Raft::ReplicationState((((this->replicationStates).at(((aer).dtor_senderId()))).nextIndex), (((this->replicationStates).at(((aer).dtor_senderId()))).matchIndex), ((((this->replicationStates).at(((aer).dtor_senderId()))).heartBeatGen)) + ((uint64)1));
      Types::TimerOutput _4_timer;
      _4_timer = Types::TimerOutput::create_TimerChanges(DafnySequence<Types::TimerCommand>::Create({Types::TimerCommand::create_SetReplicateTimer(((aer).dtor_senderId()), (((this->replicationStates).at(((aer).dtor_senderId()))).heartBeatGen))}));
      out = Types::Output<T>(_3_sends, _4_timer, _1_commits);
      return out;
    }
    out = Types::Output<T>(Types::MessageOutput <T> ::create_NoMessage(), Types::TimerOutput::create_NoTimerChange(), _1_commits);
    return out;
  }
  
  template <typename T>
  Types::Output <T>  StateMachine<T>::OnRequestVoteRpc(Types::Message <T>  rv)
  {
    Types::Output <T>  out = Types::Output<T>();
    Raft::TermRelation _0_msgTermIs;
    _0_msgTermIs = Raft::__default::CompareTerms(this->currentTerm, ((rv).dtor_term()));
    if ((_0_msgTermIs) == (Raft::TermRelation::create_Stale())) {
      out = Types::Output<T>(Types::MessageOutput <T> ::create_NoMessage(), Types::TimerOutput::create_NoTimerChange(), Types::CommitOutput::create_NoCommit());
      return out;
    }
    Types::TimerOutput _1_potentialTimer;
    _1_potentialTimer = Types::TimerOutput::create_NoTimerChange();
    if ((_0_msgTermIs) == (Raft::TermRelation::create_New())) {
      (this)->currentTerm = ((rv).dtor_term());
      (this)->votedFor = (uint64)0;
      (this)->votes = DafnySequence<uint64>::Create({});
      (this)->role = Raft::Role::create_Follower();
      DafnyArray<Raft::ReplicationState> _out0;
      _out0 = (this)->NewReplicationStates();
      (this)->replicationStates = _out0;
      (this)->timeOutGen = (this->timeOutGen) + ((uint64)1);
      _1_potentialTimer = Types::TimerOutput::create_TimerChanges(DafnySequence<Types::TimerCommand>::Create({Types::TimerCommand::create_SetElectionTimer(this->timeOutGen)}));
    }
    if (((this->votedFor) != ((uint64)0)) && ((((rv).dtor_candidateId())) != (this->votedFor))) {
      DafnySequence<Types::Send <T> > _2_sends;
      _2_sends = DafnySequence<Types::Send <T> >::Create({Types::Send<T>(((rv).dtor_candidateId()), Types::Message <T> ::create_RequestVoteResponse(this->currentTerm, false, (this)->id))});
      out = Types::Output<T>(Types::MessageOutput <T> ::create_Messages(_2_sends), _1_potentialTimer, Types::CommitOutput::create_NoCommit());
      return out;
    }
    if ((((rv).dtor_candidateId())) == (this->votedFor)) {
      DafnySequence<Types::Send <T> > _3_sends;
      _3_sends = DafnySequence<Types::Send <T> >::Create({Types::Send<T>(((rv).dtor_candidateId()), Types::Message <T> ::create_RequestVoteResponse(this->currentTerm, true, (this)->id))});
      out = Types::Output<T>(Types::MessageOutput <T> ::create_Messages(_3_sends), _1_potentialTimer, Types::CommitOutput::create_NoCommit());
      return out;
    }
    if (Raft::__default::CandidateLogUpToDate(((rv).dtor_lastLogTerm()), (((this->log).at(this->lastLogIndex)).term), ((rv).dtor_lastLogIndex()), this->lastLogIndex)) {
      (this)->votedFor = ((rv).dtor_candidateId());
      if ((_0_msgTermIs) != (Raft::TermRelation::create_New())) {
        (this)->timeOutGen = (this->timeOutGen) + ((uint64)1);
        _1_potentialTimer = Types::TimerOutput::create_TimerChanges(DafnySequence<Types::TimerCommand>::Create({Types::TimerCommand::create_SetElectionTimer(this->timeOutGen)}));
      }
      DafnySequence<Types::Send <T> > _4_sends;
      _4_sends = DafnySequence<Types::Send <T> >::Create({Types::Send<T>(((rv).dtor_candidateId()), Types::Message <T> ::create_RequestVoteResponse(this->currentTerm, true, (this)->id))});
      out = Types::Output<T>(Types::MessageOutput <T> ::create_Messages(_4_sends), _1_potentialTimer, Types::CommitOutput::create_NoCommit());
      return out;
    }
    DafnySequence<Types::Send <T> > _5_sends;
    _5_sends = DafnySequence<Types::Send <T> >::Create({Types::Send<T>(((rv).dtor_candidateId()), Types::Message <T> ::create_RequestVoteResponse(this->currentTerm, false, (this)->id))});
    out = Types::Output<T>(Types::MessageOutput <T> ::create_Messages(_5_sends), _1_potentialTimer, Types::CommitOutput::create_NoCommit());
    return out;
  }
  
  template <typename T>
  Types::Output <T>  StateMachine<T>::OnRequestVoteResponseRpc(Types::Message <T>  rvr)
  {
    Types::Output <T>  out = Types::Output<T>();
    Raft::TermRelation _0_msgTermIs;
    _0_msgTermIs = Raft::__default::CompareTerms(this->currentTerm, ((rvr).dtor_term()));
    if ((_0_msgTermIs) == (Raft::TermRelation::create_Stale())) {
      out = Types::Output<T>(Types::MessageOutput <T> ::create_NoMessage(), Types::TimerOutput::create_NoTimerChange(), Types::CommitOutput::create_NoCommit());
      return out;
    }
    if ((_0_msgTermIs) == (Raft::TermRelation::create_New())) {
      (this)->currentTerm = ((rvr).dtor_term());
      (this)->votedFor = (uint64)0;
      (this)->votes = DafnySequence<uint64>::Create({});
      (this)->role = Raft::Role::create_Follower();
      DafnyArray<Raft::ReplicationState> _out0;
      _out0 = (this)->NewReplicationStates();
      (this)->replicationStates = _out0;
      (this)->timeOutGen = (this->timeOutGen) + ((uint64)1);
      out = Types::Output<T>(Types::MessageOutput <T> ::create_NoMessage(), Types::TimerOutput::create_TimerChanges(DafnySequence<Types::TimerCommand>::Create({Types::TimerCommand::create_SetElectionTimer(this->timeOutGen)})), Types::CommitOutput::create_NoCommit());
      return out;
    }
    if (((this->role) != (Raft::Role::create_Candidate())) || (!(((rvr).dtor_voteGranted())))) {
      out = Types::Output<T>(Types::MessageOutput <T> ::create_NoMessage(), Types::TimerOutput::create_NoTimerChange(), Types::CommitOutput::create_NoCommit());
      return out;
    }
    uint64 _1_i;
    _1_i = (uint64)0;
    while ((_1_i) < ((uint64)((this->votes).size()))) {
      if ((((rvr).dtor_senderId())) == ((this->votes).select(_1_i))) {
        out = Types::Output<T>(Types::MessageOutput <T> ::create_NoMessage(), Types::TimerOutput::create_NoTimerChange(), Types::CommitOutput::create_NoCommit());
        return out;
      }
      _1_i = (_1_i) + ((uint64)1);
    }
    (this)->votes = (this->votes).concatenate(DafnySequence<uint64>::Create({((rvr).dtor_senderId())}));
    if (((uint64)((this->votes).size())) >= ((this)->quorum)) {
      uint64 _2_localLLI;
      _2_localLLI = this->lastLogIndex;
      (this)->role = Raft::Role::create_Leader();
      DafnyArray<Raft::ReplicationState> _out1;
      _out1 = (this)->NewLeaderReplicationStates(_2_localLLI);
      (this)->replicationStates = _out1;
      DafnySequence<Types::Send <T> > _3_sends;
      _3_sends = DafnySequence<Types::Send <T> >::Create({});
      DafnySequence<Types::TimerCommand> _4_timerCommands;
      _4_timerCommands = DafnySequence<Types::TimerCommand>::Create({});
      uint64 _5_i;
      _5_i = (uint64)1;
      while ((_5_i) < (((this)->total__num) + ((uint64)1))) {
        if ((_5_i) != ((this)->id)) {
          Types::Message <T>  _6_ae;
          _6_ae = Types::Message <T> ::create_AppendEntries(this->currentTerm, (this)->id, this->lastLogIndex, (((this->log).at(this->lastLogIndex)).term), DafnySequence<Types::Entry <T> >::Create({}), this->commitIndex);
          _3_sends = (_3_sends).concatenate(DafnySequence<Types::Send <T> >::Create({Types::Send<T>(_5_i, _6_ae)}));
          _4_timerCommands = (_4_timerCommands).concatenate(DafnySequence<Types::TimerCommand>::Create({Types::TimerCommand::create_SetReplicateTimer(_5_i, (((this->replicationStates).at(_5_i)).heartBeatGen))}));
        }
        _5_i = (_5_i) + ((uint64)1);
      }
      out = Types::Output<T>(Types::MessageOutput <T> ::create_Messages(_3_sends), Types::TimerOutput::create_TimerChanges(_4_timerCommands), Types::CommitOutput::create_NoCommit());
      return out;
    }
    out = Types::Output<T>(Types::MessageOutput <T> ::create_NoMessage(), Types::TimerOutput::create_NoTimerChange(), Types::CommitOutput::create_NoCommit());
    return out;
  }
  
  template <typename T>
  uint64 StateMachine<T>::LogLen()
  {
    return (this->lastLogIndex) + ((uint64)1);
  }
  
  template <typename T>
  void StateMachine<T>::TryAdvanceCommitIndex(uint64 maxIndex)
  {
    uint64 _0_newCommitIndex;
    _0_newCommitIndex = maxIndex;
    while ((_0_newCommitIndex) > (this->commitIndex)) {
      uint64 _1_matchCount;
      uint64 _out0;
      _out0 = (this)->CountReplicasWithMatchAtLeast(_0_newCommitIndex);
      _1_matchCount = _out0;
      if (((_1_matchCount) >= ((this)->quorum)) && (((((this->log).at(_0_newCommitIndex)).term)) == (this->currentTerm))) {
        (this)->commitIndex = _0_newCommitIndex;
        goto after_0;
      }
      _0_newCommitIndex = (_0_newCommitIndex) - ((uint64)1);
    }
  after_0: ;
  }
  
  template <typename T>
  uint64 StateMachine<T>::CountReplicasWithMatchAtLeast(uint64 index)
  {
    uint64 count = 0;
    count = (uint64)0;
    uint64 _0_i;
    _0_i = (uint64)1;
    uint64 _1_len;
    _1_len = (uint64)((this->replicationStates).size());
    while ((_0_i) < (_1_len)) {
      if (((((this->replicationStates).at(_0_i)).matchIndex)) >= (index)) {
        count = (count) + ((uint64)1);
      }
      _0_i = (_0_i) + ((uint64)1);
    }
    return count;
  }
  
  template <typename T>
  DafnyArray<Raft::ReplicationState> StateMachine<T>::NewReplicationStates()
  {
    DafnyArray<Raft::ReplicationState> rs = DafnyArray<Raft::ReplicationState>::Null();
    uint64 _0_size;
    _0_size = ((this)->total__num) + ((uint64)1);
    DafnyArray<Raft::ReplicationState> _nw0 = DafnyArray<Raft::ReplicationState>::New(_0_size);
    rs = _nw0;
    uint64 _1_i;
    _1_i = (uint64)0;
    while ((_1_i) < (((this)->total__num) + ((uint64)1))) {
      (rs).at((_1_i)) = Raft::__default::unusedIndex;
      _1_i = (_1_i) + ((uint64)1);
    }
    return rs;
  }
  
  template <typename T>
  DafnyArray<Raft::ReplicationState> StateMachine<T>::NewLeaderReplicationStates(uint64 lli)
  {
    DafnyArray<Raft::ReplicationState> rs = DafnyArray<Raft::ReplicationState>::Null();
    uint64 _0_size;
    _0_size = ((this)->total__num) + ((uint64)1);
    DafnyArray<Raft::ReplicationState> _nw0 = DafnyArray<Raft::ReplicationState>::New(_0_size);
    rs = _nw0;
    (rs).at((0)) = Raft::__default::unusedIndex;
    uint64 _1_i;
    _1_i = (uint64)1;
    while ((_1_i) < (((this)->total__num) + ((uint64)1))) {
      if ((_1_i) == ((this)->id)) {
        (rs).at((_1_i)) = Raft::ReplicationState((lli) + ((uint64)1), lli, (uint64)0);
      } else {
        (rs).at((_1_i)) = Raft::ReplicationState((lli) + ((uint64)1), (uint64)0, (uint64)1);
      }
      _1_i = (_1_i) + ((uint64)1);
    }
    return rs;
  }
  
  template <typename T>
  void StateMachine<T>::UpdateLog(Types::Message <T>  ae)
  {
    uint64 _0_logIdx;
    _0_logIdx = (((ae).dtor_prevLogIndex())) + ((uint64)1);
    uint64 _1_entriesIdx;
    _1_entriesIdx = (uint64)0;
    while (((_0_logIdx) < ((this)->LogLen())) && ((_1_entriesIdx) < ((uint64)((((ae).dtor_entries())).size())))) {
      if (((((((ae).dtor_entries())).select(_1_entriesIdx)).term)) != ((((this->log).at(_0_logIdx)).term))) {
        (this)->lastLogIndex = (_0_logIdx) - ((uint64)1);
        goto after_0;
      }
      _0_logIdx = (_0_logIdx) + ((uint64)1);
      _1_entriesIdx = (_1_entriesIdx) + ((uint64)1);
    }
  after_0: ;
    if ((_1_entriesIdx) < ((uint64)((((ae).dtor_entries())).size()))) {
      (this)->AppendNewEntries(ae, _1_entriesIdx);
    }
  }
  
  template <typename T>
  void StateMachine<T>::AppendNewEntries(Types::Message <T>  ae, uint64 startIdx)
  {
    uint64 _0_i;
    _0_i = startIdx;
    while ((_0_i) < ((uint64)((((ae).dtor_entries())).size()))) {
      DafnyArray<Types::Entry <T> > _arr0 = this->log;
      uint64 _index0 = (this->lastLogIndex) + ((uint64)1);
      _arr0.at(_index0) = (((ae).dtor_entries())).select(_0_i);
      (this)->lastLogIndex = (this->lastLogIndex) + ((uint64)1);
      _0_i = (_0_i) + ((uint64)1);
    }
  }
#include "NewLog.h"
}// end of namespace Raft 
namespace _module  {

}// end of namespace _module 
template <typename T>
struct get_default<Types::Entry<T> > {
  static Types::Entry<T> call() {
    return Types::Entry<T>();
  }
};
template <typename T>
struct get_default<Types::Message<T> > {
  static Types::Message<T> call() {
    return Types::Message<T>();
  }
};
template <typename T>
struct get_default<Types::Event<T> > {
  static Types::Event<T> call() {
    return Types::Event<T>();
  }
};
template <typename T>
struct get_default<Types::Send<T> > {
  static Types::Send<T> call() {
    return Types::Send<T>();
  }
};
template <typename T>
struct get_default<Types::MessageOutput<T> > {
  static Types::MessageOutput<T> call() {
    return Types::MessageOutput<T>();
  }
};
template <>
struct get_default<Types::TimerCommand > {
  static Types::TimerCommand call() {
    return Types::TimerCommand();
  }
};
template <>
struct get_default<Types::TimerOutput > {
  static Types::TimerOutput call() {
    return Types::TimerOutput();
  }
};
template <>
struct get_default<Types::CommitOutput > {
  static Types::CommitOutput call() {
    return Types::CommitOutput();
  }
};
template <typename T>
struct get_default<Types::Output<T> > {
  static Types::Output<T> call() {
    return Types::Output<T>();
  }
};
template <>
struct get_default<Raft::Role > {
  static Raft::Role call() {
    return Raft::Role();
  }
};
template <>
struct get_default<Raft::TermRelation > {
  static Raft::TermRelation call() {
    return Raft::TermRelation();
  }
};
template <>
struct get_default<Raft::ReplicationState > {
  static Raft::ReplicationState call() {
    return Raft::ReplicationState();
  }
};
template <>
struct get_default<std::shared_ptr<Types::__default > > {
static std::shared_ptr<Types::__default > call() {
return std::shared_ptr<Types::__default >();}
};
template <>
struct get_default<std::shared_ptr<Types::class_uint64 > > {
static std::shared_ptr<Types::class_uint64 > call() {
return std::shared_ptr<Types::class_uint64 >();}
};
template <>
struct get_default<std::shared_ptr<Raft::__default > > {
static std::shared_ptr<Raft::__default > call() {
return std::shared_ptr<Raft::__default >();}
};
template <typename T>
struct get_default<std::shared_ptr<Raft::StateMachine<T> > > {
static std::shared_ptr<Raft::StateMachine<T> > call() {
return std::shared_ptr<Raft::StateMachine<T> >();}
};
