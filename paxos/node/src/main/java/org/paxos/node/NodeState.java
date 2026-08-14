package org.paxos.node;

import org.paxos.proto.AcceptEntry;
import org.paxos.proto.AcceptTriplet;
import org.paxos.proto.Ballot;
import org.paxos.proto.ClientMeta;
import org.paxos.proto.ClientReply;
import org.paxos.proto.NewViewLog;
import org.paxos.proto.Request;
import org.paxos.proto.Transaction;
import org.paxos.proto.TwoPcStage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class NodeState {

    public enum Phase {
        NONE('X'),
        ACCEPTED('A'),
        COMMITTED('C'),
        EXECUTED('E');

        private final char symbol;

        Phase(char symbol) {
            this.symbol = symbol;
        }

        public char symbol() {
            return symbol;
        }
    }

    public record ExecutionInfo(long seq, boolean success, String error, Request request, Ballot ballot) {
    }

    public final int nodeNumericId;
    public final List<String> peerTargets;
    public final Map<Integer, String> nodeAddressBook;
    public final int clusterSize;

    public volatile Roles role = Roles.FOLLOWER;
    public volatile Ballot highestSeenBallot =
            Ballot.newBuilder().setRound(0).setLeaderId(0).build();
    public volatile Ballot currentLeaderBallot =
            Ballot.newBuilder().setRound(0).setLeaderId(0).build();

    public volatile long lastPrepareSeenMs = System.currentTimeMillis();
    public volatile long lastHeartbeatSeenMs = System.currentTimeMillis();

    public final ConcurrentSkipListMap<Long, LogEntry> log = new ConcurrentSkipListMap<>();

    public final ConcurrentSkipListMap<Long, ExecutionInfo> executionInfo = new ConcurrentSkipListMap<>();

    public volatile long commitIndex = 0L;
    public volatile long executedIndex   = 0L;

    //Proposer state used to track next seuqence to use as leader
    public final AtomicLong nextSeq = new AtomicLong(1L);

    public final BankStateMachine stateMachine;
    public final AccountStore accountStore;
    public volatile ShardMapper shardMapper;

    // Client reply cache & timestamp tracking
    private final ConcurrentMap<String, ClientReply> replyCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> clientHighWaterTs = new ConcurrentHashMap<>();

    private final CopyOnWriteArrayList<NewViewLog> viewHistory = new CopyOnWriteArrayList<>();

    // Coarse lock to guard multi-field transitions
    public final ReentrantReadWriteLock rw = new ReentrantReadWriteLock();

    private final Object lockGuard = new Object();
    private final java.util.Set<String> heldLocks = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final java.util.Set<Integer> modifiedIds = java.util.concurrent.ConcurrentHashMap.newKeySet();

    public static final Request NOOP = Request.newBuilder()
            .setTrans(Transaction.newBuilder()
                    .setSender("")
                    .setReceiver("")
                    .setAmount(0)
                    .build())
            .build();

    public NodeState(int nodeNumericId, List<String> peerTargets, Map<Integer, String> nodeAddressBook) {
        this.nodeNumericId = nodeNumericId;
        this.peerTargets = Collections.unmodifiableList(new ArrayList<>(peerTargets));
        this.nodeAddressBook = Collections.unmodifiableMap(new LinkedHashMap<>(nodeAddressBook));
        this.clusterSize = this.peerTargets.size() + 1;
        this.stateMachine = new BankStateMachine(nodeNumericId);
        this.accountStore = new AccountStore(nodeNumericId);
        this.shardMapper = ShardMapper.loadOrDefault(nodeNumericId);
        this.walStore = new WalStore(nodeNumericId);

        this.walStore.reset();
    }

    public boolean isQuorum(int votes) {
        return votes >= (clusterSize / 2 + 1);
    }

    public Ballot nextBallot() {
        long currentRound = highestSeenBallot.getRound();
        long round = currentRound + 1;

        return Ballot.newBuilder()
                .setRound(round)
                .setLeaderId(nodeNumericId)
                .build();
    }

    public Optional<String> addressForNode(int numericId) {
        return Optional.ofNullable(nodeAddressBook.get(numericId));
    }


    public final TwoPcManager twoPc = new TwoPcManager();
    private final WalStore walStore;

    public static final class WalRecord implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public final String txnId;
        public final int senderId;
        public final int receiverId;
        public final long amount;
        public final long oldSenderBal;
        public final long oldReceiverBal;

        WalRecord(String txnId, int senderId, int receiverId, long amount, long oldSenderBal, long oldReceiverBal) {
            this.txnId = txnId;
            this.senderId = senderId;
            this.receiverId = receiverId;
            this.amount = amount;
            this.oldSenderBal = oldSenderBal;
            this.oldReceiverBal = oldReceiverBal;
        }
    }


    public WalRecord walApplyPrepareDebit(String txnId, int senderId, long amount) {
        WalRecord existing = walStore.get(txnId);
        if (existing != null && existing.senderId == senderId) {
            return existing;
        }
        long oldBal = accountStore.getBalance(senderId);
        long newBal = oldBal - amount;
        if (newBal < 0) {
            throw new IllegalStateException("INSUFFICIENT_FUNDS");
        }
        if (!accountStore.setBalance(senderId, newBal)) {
            throw new IllegalStateException("DB_ERROR");
        }
        recordModified(senderId);
        WalRecord rec = new WalRecord(txnId, senderId, 0, amount, oldBal, 0L);
        walStore.put(txnId, rec);
        return rec;
    }


    public WalRecord walApplyPrepareCredit(String txnId, int receiverId, long amount) {
        WalRecord existing = walStore.get(txnId);
        if (existing != null && existing.receiverId == receiverId) {
            return existing;
        }
        long oldBal = accountStore.getBalance(receiverId);
        long newBal = oldBal + amount;
        if (!accountStore.setBalance(receiverId, newBal)) {
            throw new IllegalStateException("DB_ERROR");
        }
        recordModified(receiverId);
        WalRecord rec = new WalRecord(txnId, 0, receiverId, amount, 0L, oldBal);
        walStore.put(txnId, rec);
        return rec;
    }


    public void walOnCommit(String txnId) {
        walStore.remove(txnId);
    }


    public void walOnAbort(String txnId) {
        WalRecord rec = walStore.remove(txnId);
        if (rec == null) return;
        if (rec.senderId > 0) {
            accountStore.setBalance(rec.senderId, rec.oldSenderBal);
            recordModified(rec.senderId);
        }
        if (rec.receiverId > 0) {
            accountStore.setBalance(rec.receiverId, rec.oldReceiverBal);
            recordModified(rec.receiverId);
        }
    }

    public java.util.Collection<WalRecord> walValues() {
        return walStore.values();
    }


    public int clusterIndexForAccountId(int id) {
        return shardMapper.clusterIndexForAccountId(id);
    }

    public List<Integer> numericIdsByCluster(int clusterIndex) {
        List<Integer> ids = new ArrayList<>();
        for (Integer n : nodeAddressBook.keySet()) {
            int idx = ((n - 1) / 3);
            if (idx == clusterIndex) ids.add(n);
        }
        Collections.sort(ids);
        return ids;
    }

    public void markAsAccepted(long seq, Ballot ballot, Request request) {
        markAsAccepted(seq, ballot, request, TwoPcStage.TWO_PC_NONE);
    }

    public void markAsAccepted(long seq, Ballot ballot, Request request, TwoPcStage stage) {
        log.compute(seq, (k, existing) -> {
            if (existing == null) {
                return new LogEntry(request, ballot, Phase.ACCEPTED, stage == null ? TwoPcStage.TWO_PC_NONE : stage);
            }

            Phase currentPhase = existing.getPhase();
            if (currentPhase == Phase.COMMITTED || currentPhase == Phase.EXECUTED) {
                if (ballot != null && (existing.getAcceptedBallot() == null ||
                        Ballots.greater(ballot, existing.getAcceptedBallot()))) {
                    existing.setAcceptedBallot(ballot);
                }
                return existing;
            }
            if (ballot == null) {
                existing.advancePhase(Phase.ACCEPTED);
                return existing;
            }

            Ballot existingBallot = existing.getAcceptedBallot();
            TwoPcStage newStage = (stage != null && stage != TwoPcStage.TWO_PC_NONE) ? stage : existing.getTwoPcStage();

            if (existingBallot == null || Ballots.greater(ballot, existingBallot)) {
                return new LogEntry(request, ballot, Phase.ACCEPTED, newStage);
            }

            if (ballot.getRound() == existingBallot.getRound() &&
                    ballot.getLeaderId() == existingBallot.getLeaderId()) {
                if (!existing.getRequest().equals(request)) {
                    System.err.printf("[NodeState] WARNING: Received different value for same ballot! " +
                                    "seq=%d, ballot=(%d,%d). Keeping original.%n",
                            seq, ballot.getRound(), ballot.getLeaderId());
                }
                return existing;
            }

            return existing;
        });
    }

    public void markCommitted(long seq) {
        LogEntry entry = log.get(seq);
        if (entry != null) {
            entry.advancePhase(Phase.COMMITTED);
        }
    }

    public void markExecuted(long seq) {
        LogEntry entry = log.get(seq);
        if (entry != null) {
            entry.advancePhase(Phase.EXECUTED);
        }
    }

    public Phase phaseFor(long seq) {
        LogEntry entry = log.get(seq);
        return entry != null ? entry.getPhase() : Phase.NONE;
    }

    public Optional<Ballot> ballotFor(long seq) {
        LogEntry entry = log.get(seq);
        return entry != null ? Optional.ofNullable(entry.getAcceptedBallot()) : Optional.empty();
    }

    public Optional<Request> requestFor(long seq) {
        LogEntry entry = log.get(seq);
        return entry != null ? Optional.of(entry.getRequest()) : Optional.empty();
    }

    public List<AcceptTriplet> getAcceptedEntries() {
        List<AcceptTriplet> triplets = new ArrayList<>();
        log.forEach((seq, entry) -> {

            if (entry.getAcceptedBallot() != null) {
                triplets.add(AcceptTriplet.newBuilder()
                        .setB(entry.getAcceptedBallot())
                        .setS(org.paxos.proto.SeqNo.newBuilder().setValue(seq))
                        .setM(entry.getRequest())
                        .build());
            }
        });
        return triplets;
    }

    public void recordView(NewViewLog msg) {
        viewHistory.add(msg);
    }

    public List<NewViewLog> viewHistorySnapshot() {
        return List.copyOf(viewHistory);
    }

    public Optional<ExecutionInfo> executionFor(long seq) {
        return Optional.ofNullable(executionInfo.get(seq));
    }

    public void rememberExecution(ExecutionInfo info) {
        executionInfo.putIfAbsent(info.seq(), info);
        markExecuted(info.seq());
    }

    public void rememberReply(ClientReply reply) {
        if (!reply.hasMeta()) {
            return;
        }
        ClientMeta meta = reply.getMeta();
        if (meta.getClientId().isBlank()) {
            return;
        }
        String key = cacheKey(meta.getClientId(), meta.getTimestamp());
        replyCache.put(key, reply);
        clientHighWaterTs.merge(meta.getClientId(), meta.getTimestamp(), Math::max);
    }

    public boolean tryAcquireLocks(String a, String b) {
        String k1 = a == null ? "" : a;
        String k2 = b == null ? "" : b;
        if (k1.isBlank() || k2.isBlank()) {
            return false;
        }
        if (k1.equals(k2)) {
            synchronized (lockGuard) {
                if (heldLocks.contains(k1)) {
                    return false;
                }
                heldLocks.add(k1);
                return true;
            }
        }
        String first = k1.compareTo(k2) <= 0 ? k1 : k2;
        String second = first.equals(k1) ? k2 : k1;
        synchronized (lockGuard) {
            if (heldLocks.contains(first) || heldLocks.contains(second)) {
                return false;
            }
            heldLocks.add(first);
            heldLocks.add(second);
            return true;
        }
    }

    public void releaseLocks(String a, String b) {
        String k1 = a == null ? "" : a;
        String k2 = b == null ? "" : b;
        synchronized (lockGuard) {
            if (!k1.isBlank()) {
                heldLocks.remove(k1);
            }
            if (!k2.isBlank() && !k2.equals(k1)) {
                heldLocks.remove(k2);
            }
        }
    }


    public boolean tryAcquireSingleLock(String key) {
        String k = key == null ? "" : key;
        if (k.isBlank()) return false;
        synchronized (lockGuard) {
            if (heldLocks.contains(k)) return false;
            heldLocks.add(k);
            return true;
        }
    }

    public void releaseSingleLock(String key) {
        String k = key == null ? "" : key;
        synchronized (lockGuard) {
            if (!k.isBlank()) {
                heldLocks.remove(k);
            }
        }
    }

    public void clearAllLocks() {
        synchronized (lockGuard) {
            heldLocks.clear();
        }
    }

    public Optional<ClientReply> cachedReply(String clientId, long timestamp) {
        if (clientId == null || clientId.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(replyCache.get(cacheKey(clientId, timestamp)));
    }

    public OptionalLong highWaterTs(String clientId) {
        Long value = clientHighWaterTs.get(clientId);
        return value == null ? OptionalLong.empty() : OptionalLong.of(value);
    }

    public String formatLog() {
        StringBuilder sb = new StringBuilder();
        sb.append("-- Log for node ").append(nodeNumericId).append(" --\n");

        java.util.NavigableSet<Long> seqs = new java.util.TreeSet<>(log.keySet());
        seqs.addAll(executionInfo.keySet());

        for (Long seq : seqs) {
            LogEntry entry = log.get(seq);
            ExecutionInfo info = executionInfo.get(seq);

            if (entry == null && info != null) {

                Request r = info.request();
                boolean isNoop = isNoop(r);
                sb.append(String.format("#%d [%c] b=%s stage=%s %s %s",
                        seq,
                        Phase.EXECUTED.symbol(),
                        "?",
                        TwoPcStage.TWO_PC_NONE,
                        compact(r),
                        isNoop ? " (NO-OP)" : ""));
                sb.append(String.format(" -> %s", info.success() ? "SUCCESS" : "FAIL:" + info.error()));
                sb.append('\n');
                continue;
            }

            if (entry == null) {
                continue;
            }

            Phase phase = entry.getPhase();
            boolean isNoop = isNoop(entry.getRequest());
            Ballot ballot = entry.getAcceptedBallot();
            TwoPcStage stage = entry.getTwoPcStage();
            TwoPcStage effStage = (stage == null) ? TwoPcStage.TWO_PC_NONE : stage;

            if (effStage == TwoPcStage.TWO_PC_COMMIT || effStage == TwoPcStage.TWO_PC_ABORT) {
                sb.append(String.format("#%d [%c] b=%s stage=%s %s %s",
                        seq,
                        phase.symbol(),
                        ballot != null ? ballot.getRound() : "?",
                        TwoPcStage.TWO_PC_PREPARE,
                        compact(entry.getRequest()),
                        isNoop ? " (NO-OP)" : ""));
                sb.append('\n');

                sb.append(String.format("#%d [%c] b=%s stage=%s %s %s",
                        seq,
                        phase.symbol(),
                        ballot != null ? ballot.getRound() : "?",
                        effStage,
                        compact(entry.getRequest()),
                        isNoop ? " (NO-OP)" : ""));
                if (info != null) {
                    sb.append(String.format(" -> %s", info.success() ? "SUCCESS" : "FAIL:" + info.error()));
                }
                sb.append('\n');
            } else {
                sb.append(String.format("#%d [%c] b=%s stage=%s %s %s",
                        seq,
                        phase.symbol(),
                        ballot != null ? ballot.getRound() : "?",
                        effStage,
                        compact(entry.getRequest()),
                        isNoop ? " (NO-OP)" : ""));
                if (info != null) {
                    sb.append(String.format(" -> %s", info.success() ? "SUCCESS" : "FAIL:" + info.error()));
                }
                sb.append('\n');
            }
        }
        return sb.toString();
    }

    public String formatDb() {
        StringBuilder sb = new StringBuilder();
        sb.append("-- Datastore for node ").append(nodeNumericId).append(" --\n");
        java.util.List<Integer> ids = new java.util.ArrayList<>(modifiedIds);
        java.util.Collections.sort(ids);
        for (Integer id : ids) {
            long bal = accountStore.getBalance(id);
            sb.append(id).append(' ').append(bal).append('\n');
        }
        return sb.toString();
    }

    public long readBalance(int id) {
        return accountStore.getBalance(id);
    }

    public String formatViews() {
        StringBuilder sb = new StringBuilder();
        sb.append("-- View history for node ").append(nodeNumericId).append(" --\n");
        if (viewHistory.isEmpty()) {
            sb.append("<none>\n");
        } else {
            int idx = 1;
            for (NewViewLog msg : viewHistory) {
                sb.append(String.format("%d) ballot=%d leader=%s entries=%d\n",
                        idx++, msg.getB().getRound(), msg.getLeader().getId(), msg.getEntriesCount()));
                for (AcceptEntry entry : msg.getEntriesList()) {
                    long seq = entry.getS().getValue();
                    Ballot srcBallot = entry.hasSrcBallot() ? entry.getSrcBallot() : msg.getB();
                    String payload = entry.getIsNoop() ? "noop" : compact(entry.getM());
                    sb.append(String.format("    seq=%d srcBallot=%d value=%s%s\n",
                            seq,
                            srcBallot.getRound(),
                            payload,
                            entry.getIsNoop() ? " (noop)" : ""));
                }
            }
        }
        return sb.toString();
    }

    public static String cacheKey(String clientId, long timestamp) {
        return clientId + ':' + timestamp;
    }

    private static String compact(Request r) {
        return r.toString().replaceAll("\\s+", " ");
    }

    public static boolean isNoop(Request request) {
        if (request == null) {
            return true;
        }
        if (!request.hasTrans()) {
            return true;
        }
        Transaction tx = request.getTrans();
        return tx.getSender().isBlank() && tx.getReceiver().isBlank() && tx.getAmount() == 0;
    }

    public void recordModified(int id) {
        if (id > 0) {
            modifiedIds.add(id);
        }
    }

    public void clearModified() {
        modifiedIds.clear();
    }

    public void resetAllForNewSet() {
        rw.writeLock().lock();
        try {
            log.clear();
            executionInfo.clear();
            commitIndex = 0L;
            executedIndex = 0L;
            nextSeq.set(1L);
            replyCache.clear();
            clientHighWaterTs.clear();
            viewHistory.clear();
            clearAllLocks();
            modifiedIds.clear();
            twoPc.clear();
            shardMapper = ShardMapper.loadOrDefault(nodeNumericId);
        } finally {
            rw.writeLock().unlock();
        }
    }
}
