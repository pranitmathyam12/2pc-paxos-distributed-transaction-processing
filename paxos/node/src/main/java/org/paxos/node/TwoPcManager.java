package org.paxos.node;

import org.paxos.proto.Transaction;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

final class TwoPcManager {
    enum Role { COORDINATOR, PARTICIPANT }
    enum Stage { INIT, PREPARED_LOCAL, PREPARED_REMOTE, COMMIT_DECIDED, ABORT_DECIDED, COMMITTED, ABORTED }

    static final class TxnState {
        final String txnId;
        final Transaction tx;
        final String digest;
        final Role role;
        volatile Stage stage;
        volatile long prepareSeq = 0L;
        volatile long commitOrAbortSeq = 0L;
        volatile long createdMs = System.currentTimeMillis();
        volatile long updatedMs = createdMs;
        volatile boolean preparedLocal = false;
        volatile boolean preparedRemote = false;

        TxnState(String txnId, Transaction tx, String digest, Role role) {
            this.txnId = Objects.requireNonNull(txnId);
            this.tx = Objects.requireNonNull(tx);
            this.digest = digest == null ? "" : digest;
            this.role = Objects.requireNonNull(role);
            this.stage = Stage.INIT;
        }

        void touch() { updatedMs = System.currentTimeMillis(); }
    }

    private final Map<String, TxnState> byId = new ConcurrentHashMap<>();

    TxnState beginCoordinator(String txnId, Transaction tx, String digest) {
        return byId.computeIfAbsent(txnId, id -> new TxnState(id, tx, digest, Role.COORDINATOR));
    }

    TxnState beginParticipant(String txnId, Transaction tx, String digest) {
        return byId.computeIfAbsent(txnId, id -> new TxnState(id, tx, digest, Role.PARTICIPANT));
    }

    Optional<TxnState> get(String txnId) {
        return Optional.ofNullable(byId.get(txnId));
    }

    void markPreparedLocal(String txnId, long seq) {
        TxnState st = byId.get(txnId);
        if (st == null) return;
        st.preparedLocal = true;
        st.prepareSeq = st.prepareSeq == 0L ? seq : st.prepareSeq;
        if (st.role == Role.COORDINATOR) {
            if (st.preparedLocal && st.preparedRemote) st.stage = Stage.PREPARED_REMOTE;
            else st.stage = Stage.PREPARED_LOCAL;
        } else {
            st.stage = Stage.PREPARED_LOCAL;
        }
        st.touch();
    }

    void markPreparedRemote(String txnId) {
        TxnState st = byId.get(txnId);
        if (st == null) return;
        st.preparedRemote = true;
        if (st.role == Role.COORDINATOR) st.stage = Stage.PREPARED_REMOTE;
        st.touch();
    }

    void decideCommit(String txnId) {
        TxnState st = byId.get(txnId);
        if (st == null) return;
        st.stage = Stage.COMMIT_DECIDED;
        st.touch();
    }

    void decideAbort(String txnId) {
        TxnState st = byId.get(txnId);
        if (st == null) return;
        st.stage = Stage.ABORT_DECIDED;
        st.touch();
    }

    void markCommitted(String txnId, long seq) {
        TxnState st = byId.get(txnId);
        if (st == null) return;
        st.commitOrAbortSeq = st.commitOrAbortSeq == 0L ? seq : st.commitOrAbortSeq;
        st.stage = Stage.COMMITTED;
        st.touch();
    }

    void markAborted(String txnId, long seq) {
        TxnState st = byId.get(txnId);
        if (st == null) return;
        st.commitOrAbortSeq = st.commitOrAbortSeq == 0L ? seq : st.commitOrAbortSeq;
        st.stage = Stage.ABORTED;
        st.touch();
    }

    void remove(String txnId) {
        byId.remove(txnId);
    }

    void clear() {
        byId.clear();
    }
}
