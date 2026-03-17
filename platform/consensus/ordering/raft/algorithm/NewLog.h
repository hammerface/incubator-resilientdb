template <typename T>
DafnyArray<Types::Entry<T>> Raft::StateMachine<T>::NewLog(T sentinel) {
    DafnyArray<Types::Entry<T>> log = DafnyArray<Types::Entry<T>>::New(logSize);
    log.at((uint64)0) = Types::Entry<T>((uint64)0, sentinel);
    return log;
}