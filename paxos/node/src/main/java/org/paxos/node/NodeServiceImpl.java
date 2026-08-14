package org.paxos.node;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.stub.StreamObserver;
import org.paxos.proto.*;
import java.nio.file.Paths;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Semaphore;

import static org.paxos.node.Ballots.*;

public class NodeServiceImpl extends NodeServiceGrpc.NodeServiceImplBase {

    private static final long HEARTBEAT_INTERVAL_MS = Long.parseLong(System.getenv().getOrDefault("PAXOS_HEARTBEAT_INTERVAL_MS", "200"));
    private static final long FOLLOWER_TIMEOUT_MS = Long.parseLong(System.getenv().getOrDefault("PAXOS_FOLLOWER_TIMEOUT_MS", "500"));
    private static final long CATCHUP_RETRY_DELAY_MS = Long.parseLong(System.getenv().getOrDefault("PAXOS_CATCHUP_RETRY_MS", "500"));
    private static final long PREPARE_SUPPRESS_MS = Long.parseLong(System.getenv().getOrDefault("PAXOS_PREPARE_SUPPRESS_MS", "250"));
    private static final long FORWARD_TIMEOUT_MS = Long.parseLong(System.getenv().getOrDefault("PAXOS_FORWARD_TIMEOUT_MS", "400"));
    private static final Duration EXECUTION_WAIT = Duration.ofSeconds(5);
    private static final long FOLLOWER_TIMEOUT_JITTER_MS = Long.parseLong(
            System.getenv().getOrDefault("PAXOS_FOLLOWER_TIMEOUT_JITTER_MS", "200"));
    private static final long PROGRESS_STALL_MS = Long.parseLong(
            System.getenv().getOrDefault("PAXOS_PROGRESS_STALL_MS", "2000"));
    private static final long TWO_PC_LOCK_WAIT_TIMEOUT_MS = Long.parseLong(
            System.getenv().getOrDefault("PAXOS_2PC_LOCK_WAIT_TIMEOUT_MS", "1000"));
    private static final long TWO_PC_LOCK_RETRY_INTERVAL_MS = Long.parseLong(
            System.getenv().getOrDefault("PAXOS_2PC_LOCK_RETRY_INTERVAL_MS", "100"));
    private static final long TWO_PC_DRIVE_INTERVAL_MS = Long.parseLong(
            System.getenv().getOrDefault("PAXOS_2PC_DRIVE_INTERVAL_MS", "250"));

    // Enable/disable periodic metrics logging without affecting behavior
    private static final boolean PAXOS_METRICS_ENABLE = Boolean.parseBoolean(
            System.getenv().getOrDefault("PAXOS_METRICS_ENABLE", "true"));
    // Interval (ms) to emit metrics lines (throughput, acks, applies, backlog)
    private static final long PAXOS_METRICS_INTERVAL_MS = Long.parseLong(
            System.getenv().getOrDefault("PAXOS_METRICS_INTERVAL_MS", "1000"));

    // Enable async gRPC for Accept/Commit on the leader; early finish on majority acks
    private static final boolean PAXOS_ASYNC_RPC = Boolean.parseBoolean(
            System.getenv().getOrDefault("PAXOS_ASYNC_RPC", "false"));
    // Allow multiple log slots in-flight at the leader (replication pipelining)
    // Default false keeps one-at-a-time replication
    private static final boolean PAXOS_PIPELINING = Boolean.parseBoolean(
            System.getenv().getOrDefault("PAXOS_PIPELINING", "false"));
    // Max concurrent in-flight slots when pipelining is enabled
    private static final int PAXOS_PIPELINE_DEPTH = Integer.parseInt(
            System.getenv().getOrDefault("PAXOS_PIPELINE_DEPTH", "16"));
    // Process client submits on a small worker pool; 1 preserves current single-thread behavior
    private static final int PAXOS_SUBMIT_WORKERS = Integer.parseInt(
            System.getenv().getOrDefault("PAXOS_SUBMIT_WORKERS", "1"));

    // Default false preserves current execution location
    private static final boolean PAXOS_APPLIER_THREAD = Boolean.parseBoolean(
            System.getenv().getOrDefault("PAXOS_APPLIER_THREAD", "false"));

    // Used by future flow control; informational in this step
    private static final long PAXOS_EXEC_BACKLOG_MAX = Long.parseLong(
            System.getenv().getOrDefault("PAXOS_EXEC_BACKLOG_MAX", "256"));

    // Default false preserves current stall-demotion behavior
    private static final boolean PAXOS_STALL_DEMOTE_CONSERVATIVE = Boolean.parseBoolean(
            System.getenv().getOrDefault("PAXOS_STALL_DEMOTE_CONSERVATIVE", "false"));

    private static final boolean PAXOS_RPC_TCP_NODELAY = Boolean.parseBoolean(
            System.getenv().getOrDefault("PAXOS_RPC_TCP_NODELAY", "true"));
    private static final long PAXOS_RPC_KEEPALIVE_TIME_SEC = Long.parseLong(
            System.getenv().getOrDefault("PAXOS_RPC_KEEPALIVE_TIME_SEC", "30"));
    private static final long PAXOS_RPC_KEEPALIVE_TIMEOUT_SEC = Long.parseLong(
            System.getenv().getOrDefault("PAXOS_RPC_KEEPALIVE_TIMEOUT_SEC", "10"));
    private static final boolean PAXOS_RPC_KEEPALIVE_WITHOUT_CALLS = Boolean.parseBoolean(
            System.getenv().getOrDefault("PAXOS_RPC_KEEPALIVE_WITHOUT_CALLS", "true"));
    private static final int PAXOS_RPC_MAX_INBOUND_MESSAGE_MB = Integer.parseInt(
            System.getenv().getOrDefault("PAXOS_RPC_MAX_INBOUND_MESSAGE_MB", "8"));
    private static final int PAXOS_RPC_FLOW_CONTROL_WINDOW_BYTES = Integer.parseInt(
            System.getenv().getOrDefault("PAXOS_RPC_FLOW_CONTROL_WINDOW_BYTES", "1048576"));
    private static final boolean PAXOS_RPC_ENABLE_GZIP = Boolean.parseBoolean(
            System.getenv().getOrDefault("PAXOS_RPC_ENABLE_GZIP", "false"));
    private static final String PAXOS_RPC_COMPRESSION_MODE = System.getenv().getOrDefault("PAXOS_RPC_COMPRESSION_MODE", "off");
    private static final int PAXOS_RPC_COMPRESSION_THRESHOLD_BYTES = Integer.parseInt(
            System.getenv().getOrDefault("PAXOS_RPC_COMPRESSION_THRESHOLD_BYTES", "65536"));

    private static final boolean PAXOS_VERBOSE_LOGS = Boolean.parseBoolean(
            System.getenv().getOrDefault("PAXOS_VERBOSE_LOGS", "true"));

    private static final boolean PAXOS_SNAPSHOT_ENABLE = Boolean.parseBoolean(
            System.getenv().getOrDefault("PAXOS_SNAPSHOT_ENABLE", "false"));
    private static final long PAXOS_SNAPSHOT_INTERVAL_MS = Long.parseLong(
            System.getenv().getOrDefault("PAXOS_SNAPSHOT_INTERVAL_MS", "10000"));
    private static final String PAXOS_SNAPSHOT_DIR = System.getenv().getOrDefault("PAXOS_SNAPSHOT_DIR", "snapshots");
    private static final boolean PAXOS_COMPACTION_ENABLE = Boolean.parseBoolean(
            System.getenv().getOrDefault("PAXOS_COMPACTION_ENABLE", "false"));
    private static final long PAXOS_COMPACTION_RETAIN = Long.parseLong(
            System.getenv().getOrDefault("PAXOS_COMPACTION_RETAIN", "1000"));
    private static final boolean PAXOS_SNAPSHOT_INSTALL_ENABLE = Boolean.parseBoolean(
            System.getenv().getOrDefault("PAXOS_SNAPSHOT_INSTALL_ENABLE", "false"));

    private final long effectiveFollowerTimeout;
    private final AtomicLong lastElectionAttempt = new AtomicLong(0);
    private final AtomicLong lastProgressMs = new AtomicLong(System.currentTimeMillis());

    private final NodeState st;
    private final List<NodeServiceGrpc.NodeServiceBlockingStub> peerStubs;
    private final ConcurrentMap<Long, CompletableFuture<NodeState.ExecutionInfo>> executionWaiters = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, NodeServiceGrpc.NodeServiceBlockingStub> catchupStubCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, ManagedChannel> catchupChannels = new ConcurrentHashMap<>();
    // Cross-cluster addressing for 2PC control RPCs
    private final ConcurrentMap<Integer, NodeServiceGrpc.NodeServiceBlockingStub> remoteRpcStubCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, ManagedChannel> remoteRpcChannels = new ConcurrentHashMap<>();
    private final AtomicReference<DeferredPrepare> deferredPrepare = new AtomicReference<>();
    private final ScheduledExecutorService scheduler;
    private final ExecutorService catchupExecutor;
    private final ExecutorService requestExecutor;
    private final ExecutorService replicationExecutor;
    private final Semaphore pipelineSem;
    private final ExecutorService applierExecutor;
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private final AtomicBoolean active = new AtomicBoolean(true);
    private volatile ScheduledFuture<?> deferredPrepareFuture;
    private volatile long lastHeartbeatBroadcastMs = 0L;
    private volatile long lastTwoPcDriveMs = 0L;
    private final ConcurrentMap<String, CompletableFuture<ClientReply>> pendingRequests = new ConcurrentHashMap<>();


    private final java.util.concurrent.atomic.AtomicLong mSubmitRequests = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong mForwardedRequests = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong mAcceptSent = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong mAcceptAcks = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong mCommitSent = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong mExecApplied = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong mRpcErrors = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong mHeartbeatsRecv = new java.util.concurrent.atomic.AtomicLong();

    public static NodeServiceImpl withTargets(NodeState st, List<String> peerTargets, boolean plaintext) {
        List<NodeServiceGrpc.NodeServiceBlockingStub> stubs = new ArrayList<>();
        for (String target : peerTargets) {
            NettyChannelBuilder builder = NettyChannelBuilder.forTarget(target);
            if (plaintext) {
                builder.usePlaintext();
            }
            builder.keepAliveTime(PAXOS_RPC_KEEPALIVE_TIME_SEC, TimeUnit.SECONDS)
                    .keepAliveTimeout(PAXOS_RPC_KEEPALIVE_TIMEOUT_SEC, TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(PAXOS_RPC_KEEPALIVE_WITHOUT_CALLS)
                    .maxInboundMessageSize(PAXOS_RPC_MAX_INBOUND_MESSAGE_MB * 1024 * 1024)
                    .flowControlWindow(PAXOS_RPC_FLOW_CONTROL_WINDOW_BYTES)
                    .withOption(ChannelOption.TCP_NODELAY, PAXOS_RPC_TCP_NODELAY);
            ManagedChannel ch = builder.build();
            NodeServiceGrpc.NodeServiceBlockingStub stub = NodeServiceGrpc.newBlockingStub(ch);
            if (PAXOS_RPC_ENABLE_GZIP) {
                stub = stub.withCompression("gzip");
            }
            stubs.add(stub);
        }
        return new NodeServiceImpl(st, stubs);
    }

    public NodeServiceImpl(NodeState state, List<NodeServiceGrpc.NodeServiceBlockingStub> peerStubs) {
        this.st = state;
        this.peerStubs = new CopyOnWriteArrayList<>(peerStubs);

        this.effectiveFollowerTimeout = FOLLOWER_TIMEOUT_MS +
                (long)(Math.random() * FOLLOWER_TIMEOUT_JITTER_MS);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "paxos-node-" + state.nodeNumericId + "-scheduler");
            t.setDaemon(true);
            return t;
        });
        this.catchupExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "paxos-node-" + state.nodeNumericId + "-catchup");
            t.setDaemon(true);
            return t;
        });
        if (PAXOS_SUBMIT_WORKERS <= 1) {
            this.requestExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "paxos-node-" + state.nodeNumericId + "-requests");
                t.setDaemon(true);
                return t;
            });
        } else {
            this.requestExecutor = Executors.newFixedThreadPool(PAXOS_SUBMIT_WORKERS, r -> {
                Thread t = new Thread(r, "paxos-node-" + state.nodeNumericId + "-requests");
                t.setDaemon(true);
                return t;
            });
        }
        int replThreads = Math.max(2, Math.max(2, this.peerStubs.size()));
        this.replicationExecutor = Executors.newFixedThreadPool(replThreads, r -> {
            Thread t = new Thread(r, "paxos-node-" + state.nodeNumericId + "-repl");
            t.setDaemon(true);
            return t;
        });
        this.pipelineSem = new Semaphore(PAXOS_PIPELINING ? Math.max(1, PAXOS_PIPELINE_DEPTH) : 1);
        if (PAXOS_APPLIER_THREAD) {
            this.applierExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "paxos-node-" + state.nodeNumericId + "-applier");
                t.setDaemon(true);
                return t;
            });
            this.applierExecutor.execute(this::applyCommittedLoop);
        } else {
            this.applierExecutor = null;
        }
        scheduler.scheduleAtFixedRate(this::tick, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);

        scheduler.execute(this::rebuildTwoPcOnStartup);
        if (PAXOS_METRICS_ENABLE && PAXOS_METRICS_INTERVAL_MS > 0) {
            scheduler.scheduleAtFixedRate(this::reportMetrics, PAXOS_METRICS_INTERVAL_MS, PAXOS_METRICS_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }
        if (PAXOS_SNAPSHOT_ENABLE && PAXOS_SNAPSHOT_INTERVAL_MS > 0) {
            scheduler.scheduleAtFixedRate(this::backgroundSnapshot, PAXOS_SNAPSHOT_INTERVAL_MS, PAXOS_SNAPSHOT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }
        if (PAXOS_COMPACTION_ENABLE && PAXOS_SNAPSHOT_INTERVAL_MS > 0) {
            scheduler.scheduleAtFixedRate(this::backgroundCompact, PAXOS_SNAPSHOT_INTERVAL_MS * 2, PAXOS_SNAPSHOT_INTERVAL_MS * 2, TimeUnit.MILLISECONDS);
        }
    }


    private NodeServiceGrpc.NodeServiceBlockingStub rpcStubForNode(int numericId) {
        NodeServiceGrpc.NodeServiceBlockingStub cached = remoteRpcStubCache.get(numericId);
        if (cached != null) {
            return cached;
        }
        return st.addressForNode(numericId)
                .map(target -> {
                    ManagedChannel ch = NettyChannelBuilder.forTarget(target)
                            .usePlaintext()
                            .keepAliveTime(PAXOS_RPC_KEEPALIVE_TIME_SEC, TimeUnit.SECONDS)
                            .keepAliveTimeout(PAXOS_RPC_KEEPALIVE_TIMEOUT_SEC, TimeUnit.SECONDS)
                            .keepAliveWithoutCalls(PAXOS_RPC_KEEPALIVE_WITHOUT_CALLS)
                            .maxInboundMessageSize(PAXOS_RPC_MAX_INBOUND_MESSAGE_MB * 1024 * 1024)
                            .flowControlWindow(PAXOS_RPC_FLOW_CONTROL_WINDOW_BYTES)
                            .withOption(ChannelOption.TCP_NODELAY, PAXOS_RPC_TCP_NODELAY)
                            .build();
                    NodeServiceGrpc.NodeServiceBlockingStub stub = NodeServiceGrpc.newBlockingStub(ch);
                    if (PAXOS_RPC_ENABLE_GZIP) {
                        stub = stub.withCompression("gzip");
                    }
                    ManagedChannel prevCh = remoteRpcChannels.putIfAbsent(numericId, ch);
                    NodeServiceGrpc.NodeServiceBlockingStub prevStub = remoteRpcStubCache.putIfAbsent(numericId, stub);
                    if (prevCh != null) {

                        ch.shutdown();
                        try {
                            ch.awaitTermination(50, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        } finally {
                            ch.shutdownNow();
                        }
                        return prevStub;
                    }
                    return stub;
                })
                .orElse(null);
    }

    public void shutdown() {
        scheduler.shutdownNow();
        catchupExecutor.shutdownNow();
        requestExecutor.shutdownNow();
        if (replicationExecutor != null) {
            replicationExecutor.shutdownNow();
        }
        if (applierExecutor != null) {
            applierExecutor.shutdownNow();
        }


        for (Map.Entry<String, CompletableFuture<ClientReply>> entry : pendingRequests.entrySet()) {
            if (!entry.getValue().isDone()) {
                entry.getValue().completeExceptionally(new IllegalStateException("Node shutting down"));
            }
        }
        pendingRequests.clear();

        catchupChannels.values().forEach(ch -> {
            ch.shutdown();
            try {
                if (!ch.awaitTermination(200, TimeUnit.MILLISECONDS)) {
                    ch.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                ch.shutdownNow();
            }
        });
        catchupChannels.clear();
        catchupStubCache.clear();

        remoteRpcChannels.values().forEach(ch -> {
            ch.shutdown();
            try {
                if (!ch.awaitTermination(200, TimeUnit.MILLISECONDS)) {
                    ch.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                ch.shutdownNow();
            }
        });
        remoteRpcChannels.clear();
        remoteRpcStubCache.clear();

        DeferredPrepare pending = deferredPrepare.getAndSet(null);
        if (pending != null) {
            pending.resp.onError(Status.UNAVAILABLE.withDescription("NODE_INACTIVE").asRuntimeException());
        }
        cancelDeferredPrepareTask();
        failPendingWaiters("shutdown");
    }

    private void reportMetrics() {
        if (!PAXOS_METRICS_ENABLE) {
            return;
        }
        long submits = mSubmitRequests.getAndSet(0);
        long forwards = mForwardedRequests.getAndSet(0);
        long accSent = mAcceptSent.getAndSet(0);
        long accAcks = mAcceptAcks.getAndSet(0);
        long cmSent = mCommitSent.getAndSet(0);
        long applied = mExecApplied.getAndSet(0);
        long rpcErr = mRpcErrors.getAndSet(0);
        long hb = mHeartbeatsRecv.getAndSet(0);

        long execIdx;
        long commitIdx;
        st.rw.readLock().lock();
        try {
            execIdx = st.executedIndex;
            commitIdx = st.commitIndex;
        } finally {
            st.rw.readLock().unlock();
        }
        long backlog = Math.max(0, commitIdx - execIdx);

        System.out.printf("[node %d] METRICS | submit=%d fwd=%d | accept=%d/%d | commitSent=%d | applied=%d | backlog=%d | rpcErr=%d | hb=%d%n",
                st.nodeNumericId, submits, forwards, accSent, accAcks, cmSent, applied, backlog, rpcErr, hb);
    }

    private void backgroundSnapshot() {
        if (!active.get()) return;
        long executed;
        st.rw.readLock().lock();
        try {
            executed = st.executedIndex;
        } finally {
            st.rw.readLock().unlock();
        }
        try {
            st.accountStore.snapshotTo(Paths.get(PAXOS_SNAPSHOT_DIR), executed);
        } catch (Exception ignored) {}
    }

    private void backgroundCompact() {
        if (!active.get() || !PAXOS_COMPACTION_ENABLE) return;
        long executed;
        st.rw.writeLock().lock();
        try {
            executed = st.executedIndex;
            long cutoff = Math.max(0L, executed - Math.max(0L, PAXOS_COMPACTION_RETAIN));
            if (cutoff <= 0) return;
            java.util.NavigableMap<Long, LogEntry> head = st.log.headMap(cutoff, true);
            if (!head.isEmpty()) {
                head.clear();
            }
        } finally {
            st.rw.writeLock().unlock();
        }
    }

    private void tick() {
        long now = System.currentTimeMillis();
        if (!active.get()) {
            return;
        }
        processDeferredPrepare(now);

        if (st.role == Roles.LEADER) {
            long execIdx;
            long commitIdx;
            st.rw.readLock().lock();
            try {
                execIdx = st.executedIndex;
                commitIdx = st.commitIndex;
            } finally {
                st.rw.readLock().unlock();
            }
            long backlog = Math.max(0, commitIdx - execIdx);
            if (execIdx < commitIdx && now - lastProgressMs.get() >= PROGRESS_STALL_MS) {

                if (!PAXOS_STALL_DEMOTE_CONSERVATIVE || backlog <= 3) {
                    st.rw.writeLock().lock();
                    try {
                        st.role = Roles.FOLLOWER;
                    } finally {
                        st.rw.writeLock().unlock();
                    }
                    return;
                }
            }
            st.lastHeartbeatSeenMs = now;
            if (now - lastHeartbeatBroadcastMs >= HEARTBEAT_INTERVAL_MS) {
                if (broadcastHeartbeat()) {
                    lastHeartbeatBroadcastMs = now;
                }
            }
            if (now - lastTwoPcDriveMs >= TWO_PC_DRIVE_INTERVAL_MS) {
                resumeTwoPcAsLeader();
                lastTwoPcDriveMs = now;
            }
        } else {

            long elapsed = now - st.lastHeartbeatSeenMs;
            if (elapsed >= effectiveFollowerTimeout &&
                    now - st.lastPrepareSeenMs >= PREPARE_SUPPRESS_MS &&
                    canStartElection(now) &&
                    electionInProgress.compareAndSet(false, true)) {

                scheduler.execute(() -> {
                    try {
                        boolean won = startElectionWithRetries();
                        if (!won) {
                            st.lastHeartbeatSeenMs = System.currentTimeMillis();
                        }
                    } finally {
                        electionInProgress.set(false);
                    }
                });
            }
        }
    }

    //make sure the difference between two consecutive elections be more than a second
    private boolean canStartElection(long currentTime) {
        long timeSinceLastAttempt = currentTime - lastElectionAttempt.get();

        if (timeSinceLastAttempt < 1000) {
            return false;
        }

        lastElectionAttempt.set(currentTime);
        return true;
    }

    @Override
    public void prepare(PrepareMsg req, StreamObserver<PromiseMsg> resp) {
        if (!ensureActive(resp)) {
            return;
        }
        long now = System.currentTimeMillis();
        st.lastPrepareSeenMs = now;

        Ballot incoming = req.getB();

        if (st.role == Roles.LEADER && st.currentLeaderBallot.getRound() > 0) {
            long ballotDiff = incoming.getRound() - st.currentLeaderBallot.getRound();

            // if is an active leader and the incoming ballot is only slightly higher than leader ballot.

            if (ballotDiff > 0 && ballotDiff <= 2) {
                if (PAXOS_VERBOSE_LOGS) {
                    System.out.printf("[node %d] Active leader rejecting close ballot %d (current leader ballot %d, diff=%d)%n",
                            st.nodeNumericId, incoming.getRound(),
                            st.currentLeaderBallot.getRound(), ballotDiff);
                }


                PromiseMsg reject = PromiseMsg.newBuilder()
                        .setB(st.highestSeenBallot)
                        .setFrom(NodeId.newBuilder().setId(String.valueOf(st.nodeNumericId)))
                        .setHighestSeen(st.highestSeenBallot)
                        .build();
                resp.onNext(reject);
                resp.onCompleted();
                return;
            }
        }

        boolean alreadyPromised = incoming.getRound() == st.highestSeenBallot.getRound()
                && incoming.getLeaderId() == st.highestSeenBallot.getLeaderId();

        if (!alreadyPromised && maybeDeferPrepare(req, resp, now)) {
            return;
        }

        respondToPrepare(resp, incoming, alreadyPromised);
    }

    private void respondToPrepare(StreamObserver<PromiseMsg> resp, Ballot incoming, boolean alreadyPromised) {
        boolean willPromise = alreadyPromised || greaterOrEqualTo(incoming, st.highestSeenBallot);
        if (PAXOS_VERBOSE_LOGS) {
            System.out.printf("[node %d] PREPARE b=%d | myHighest=%d -> %s%n",
                    st.nodeNumericId,
                    incoming.getRound(),
                    st.highestSeenBallot.getRound(),
                    willPromise ? (alreadyPromised ? "RE-PROMISE" : "PROMISE") : "NO-ACK");
        }

        st.rw.writeLock().lock();
        try {
            if (!willPromise) {
                PromiseMsg reject = PromiseMsg.newBuilder()
                        .setB(st.highestSeenBallot)
                        .setFrom(NodeId.newBuilder().setId(String.valueOf(st.nodeNumericId)))
                        .setHighestSeen(st.highestSeenBallot)
                        .build();
                resp.onNext(reject);
                resp.onCompleted();
                return;
            }

            if (greater(incoming, st.highestSeenBallot)) {
                st.highestSeenBallot = incoming;
            }

            PromiseMsg.Builder promise = PromiseMsg.newBuilder()
                    .setB(incoming)
                    .setFrom(NodeId.newBuilder().setId(String.valueOf(st.nodeNumericId)));
            promise.setHighestSeen(st.highestSeenBallot);

            List<AcceptTriplet> acceptedEntries = st.getAcceptedEntries();
            promise.addAllAcceptLog(acceptedEntries);

            resp.onNext(promise.build());
            resp.onCompleted();
        } finally {
            st.rw.writeLock().unlock();
        }
    }

    private boolean maybeDeferPrepare(PrepareMsg req, StreamObserver<PromiseMsg> resp, long now) {
        if (st.role == Roles.LEADER) {
            return false;
        }
        if (followerTimerExpired(now)) {
            return false;
        }

        DeferredPrepare candidate = new DeferredPrepare(req, resp);
        while (true) {
            DeferredPrepare current = deferredPrepare.get();
            if (current == null) {
                if (deferredPrepare.compareAndSet(null, candidate)) {
                    scheduleDeferredPromise(now);
                    if (PAXOS_VERBOSE_LOGS) {
                        System.out.printf("[node %d] Deferring PREPARE b=%d (timer not expired)%n",
                                st.nodeNumericId, req.getB().getRound());
                    }
                    return true;
                }
                continue;
            }

            Ballot currentBallot = current.req.getB();
            if (greater(req.getB(), currentBallot)) {
                if (current.markCompleted()) {
                    rejectPrepare(current.resp);
                }
                if (deferredPrepare.compareAndSet(current, candidate)) {
                    scheduleDeferredPromise(now);
                    if (PAXOS_VERBOSE_LOGS) {
                        System.out.printf("[node %d] Replacing deferred PREPARE with higher ballot b=%d%n",
                                st.nodeNumericId, req.getB().getRound());
                    }
                    return true;
                }
                continue;
            }
            rejectPrepare(resp);
            return true;
        }
    }

    private void rejectPrepare(StreamObserver<PromiseMsg> resp) {
        try {
            PromiseMsg reject = PromiseMsg.newBuilder()
                    .setB(st.highestSeenBallot)
                    .setFrom(NodeId.newBuilder().setId(String.valueOf(st.nodeNumericId)))
                    .setHighestSeen(st.highestSeenBallot)
                    .build();
            resp.onNext(reject);
            resp.onCompleted();
        } catch (IllegalStateException e) {}
    }

    private void scheduleDeferredPromise(long now) {
        long elapsed = now - st.lastHeartbeatSeenMs;
        long remaining = FOLLOWER_TIMEOUT_MS - Math.max(elapsed, 0L);
        if (remaining <= 0) {
            scheduler.execute(this::flushDeferredPrepare);
            return;
        }
        cancelDeferredPrepareTask();
        deferredPrepareFuture = scheduler.schedule(this::flushDeferredPrepare, remaining, TimeUnit.MILLISECONDS);
    }

    private void cancelDeferredPrepareTask() {
        ScheduledFuture<?> future = deferredPrepareFuture;
        if (future != null && !future.isDone()) {
            future.cancel(false);
        }
        deferredPrepareFuture = null;
    }

    private void flushDeferredPrepare() {
        DeferredPrepare pending = deferredPrepare.getAndSet(null);
        cancelDeferredPrepareTask();
        if (pending == null) {
            return;
        }

        long now = System.currentTimeMillis();
        if (!followerTimerExpired(now) && st.role != Roles.LEADER) {
            if (deferredPrepare.compareAndSet(null, pending)) {
                scheduleDeferredPromise(now);
                return;
            }
            if (pending.markCompleted()) {
                rejectPrepare(pending.resp);
            }
            return;
        }

        if (!ensureActive(pending.resp)) {
            pending.markCompleted();
            return;
        }

        if (pending.markCompleted()) {
            Ballot ballot = pending.req.getB();
            boolean alreadyPromised = ballot.getRound() == st.highestSeenBallot.getRound()
                    && ballot.getLeaderId() == st.highestSeenBallot.getLeaderId();
            respondToPrepare(pending.resp, ballot, alreadyPromised);
        }
    }

    private void processDeferredPrepare(long now) {
        if (deferredPrepare.get() != null && (st.role == Roles.LEADER || followerTimerExpired(now))) {
            flushDeferredPrepare();
        }
    }

    private boolean followerTimerExpired(long now) {
        return now - st.lastHeartbeatSeenMs >= effectiveFollowerTimeout;
    }

    private boolean tryAcquireSingleLockWithTimer(String key) {
        long deadline = System.currentTimeMillis() + Math.max(0L, TWO_PC_LOCK_WAIT_TIMEOUT_MS);
        while (System.currentTimeMillis() < deadline && active.get()) {
            if (st.tryAcquireSingleLock(key)) {
                return true;
            }
            try {
                Thread.sleep(Math.max(1L, TWO_PC_LOCK_RETRY_INTERVAL_MS));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return st.tryAcquireSingleLock(key);
    }

    @Override
    public void submit(ClientRequest req, StreamObserver<ClientReply> resp) {
        if (!ensureActive(resp)) {
            return;
        }

        ClientMeta meta = req.hasMeta() ? req.getMeta() : ClientMeta.getDefaultInstance();
        Transaction transaction = req.hasTx() ? req.getTx() : Transaction.getDefaultInstance();
        logInboundClientRequest(meta, transaction);

        if (meta.getClientId().isBlank() || meta.getTimestamp() <= 0) {
            resp.onNext(failureReply(meta, "INVALID_META"));
            resp.onCompleted();
            return;
        }


        Optional<ClientReply> cached = st.cachedReply(meta.getClientId(), meta.getTimestamp());
        if (cached.isPresent()) {
            resp.onNext(cached.get());
            resp.onCompleted();
            return;
        }

        // checking stale timestamp
        OptionalLong highWater = st.highWaterTs(meta.getClientId());
        if (highWater.isPresent() && meta.getTimestamp() < highWater.getAsLong()) {
            resp.onNext(failureReply(meta, "STALE_TIMESTAMP"));
            resp.onCompleted();
            return;
        }

        // leader forwarding
        if (st.role != Roles.LEADER) {
            mForwardedRequests.incrementAndGet();
            Optional<ClientReply> forwarded = forwardRequesttoLeader(req, meta);
            if (forwarded.isPresent()) {
                resp.onNext(forwarded.get());
            } else {
                resp.onNext(redirectToLeader(meta));
            }
            resp.onCompleted();
            return;
        }

        // handling deduplication if you are a leader
        mSubmitRequests.incrementAndGet();
        ClientReply reply = handleSubmitAsLeaderWithDedup(req, meta);
        resp.onNext(reply);
        resp.onCompleted();
    }

    //used chatGPT to understand and genrate a part of the function
    private ClientReply handleSubmitAsLeaderWithDedup(ClientRequest req, ClientMeta meta) {
        String requestKey = NodeState.cacheKey(meta.getClientId(), meta.getTimestamp());

        Optional<ClientReply> cached = st.cachedReply(meta.getClientId(), meta.getTimestamp());
        if (cached.isPresent()) {
            return cached.get();
        }

        CompletableFuture<ClientReply> future = pendingRequests.computeIfAbsent(requestKey, k -> {
            CompletableFuture<ClientReply> newFuture = new CompletableFuture<>();
            requestExecutor.execute(() -> {
                try {

                    waitForBacklogBelow(PAXOS_EXEC_BACKLOG_MAX, FORWARD_TIMEOUT_MS);
                    Optional<ClientReply> doubleCheck = st.cachedReply(meta.getClientId(), meta.getTimestamp());
                    if (doubleCheck.isPresent()) {
                        newFuture.complete(doubleCheck.get());
                        return;
                    }

                    ClientReply result = handleSubmitAsLeader(req, meta);
                    newFuture.complete(result);
                } catch (Exception ex) {
                    System.out.printf("[node %d] Error processing request client=%s ts=%d: %s%n",
                            st.nodeNumericId, meta.getClientId(), meta.getTimestamp(), ex.getMessage());
                    newFuture.complete(failureReply(meta, "INTERNAL_ERROR"));
                } finally {
                    pendingRequests.remove(requestKey);
                }
            });
            return newFuture;
        });

        try {
            return future.get(EXECUTION_WAIT.toMillis() + 1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pendingRequests.remove(requestKey);
            return failureReply(meta, "INTERRUPTED");
        } catch (ExecutionException e) {
            pendingRequests.remove(requestKey);
            return failureReply(meta, "EXECUTION_ERROR");
        } catch (TimeoutException e) {
            System.out.printf("[node %d] Timeout client=%s ts=%d%n",
                    st.nodeNumericId, meta.getClientId(), meta.getTimestamp());
            return failureReply(meta, "PROCESSING_TIMEOUT");
        }
    }

    private void logInboundClientRequest(ClientMeta meta, Transaction transaction) {
        String clientLabel = meta.getClientId().isEmpty() ? "?" : meta.getClientId();
        long timestamp = meta.getTimestamp();
        String sender = transaction.getSender().isEmpty() ? "?" : transaction.getSender();
        String receiver = transaction.getReceiver().isEmpty() ? "?" : transaction.getReceiver();
        long amount = transaction.getAmount();
        if (PAXOS_VERBOSE_LOGS) {
            System.out.printf("[node %d] Received client=%s ts=%d sender=%s receiver=%s amount=%d%n",
                    st.nodeNumericId, clientLabel, timestamp, sender, receiver, amount);
        }
    }

    @Override
    public void accept(AcceptMsg req, StreamObserver<AcceptedMsg> resp) {
        if (!ensureActive(resp)) {
            return;
        }

        st.rw.writeLock().lock();
        try {
            if (!greaterOrEqualTo(req.getB(), st.highestSeenBallot)) {
                AcceptedMsg reject = AcceptedMsg.newBuilder()
                        .setB(st.highestSeenBallot)
                        .setS(req.getS())
                        .setFrom(NodeId.newBuilder().setId(String.valueOf(st.nodeNumericId)))
                        .build();
                resp.onNext(reject);
                resp.onCompleted();
                return;
            }

            st.highestSeenBallot = req.getB();
            st.currentLeaderBallot = req.getB();
            stageAccept(req);
            st.lastHeartbeatSeenMs = System.currentTimeMillis();

            if (PAXOS_VERBOSE_LOGS) {
                System.out.printf("[node %d] ACCEPTED s=%d @b=%d value=%s%n",
                        st.nodeNumericId,
                        req.getS().getValue(),
                        req.getB().getRound(),
                        pretty(req.getM()));
            }

            AcceptedMsg ack = AcceptedMsg.newBuilder()
                    .setB(req.getB())
                    .setS(req.getS())
                    .setFrom(NodeId.newBuilder().setId(String.valueOf(st.nodeNumericId)))
                    .build();
            resp.onNext(ack);
            resp.onCompleted();
        } finally {
            st.rw.writeLock().unlock();
        }
    }

    @Override
    public void heartbeat(HeartbeatMsg req, StreamObserver<Ack> resp) {
        if (!ensureActive(resp)) {
            return;
        }

        long now = System.currentTimeMillis();

        st.rw.writeLock().lock();
        try {
            if (greater(req.getB(), st.highestSeenBallot)) {
                st.highestSeenBallot = req.getB();
            }
            st.currentLeaderBallot = req.getB();
            st.lastHeartbeatSeenMs = now;
        } finally {
            st.rw.writeLock().unlock();
        }


        long leaderCommitIndex = req.getCommitIndex();
        long myCommitIndex = st.commitIndex;

        if (leaderCommitIndex > myCommitIndex) {
            long gap = leaderCommitIndex - myCommitIndex;
            System.out.printf("[node %d] Heartbeat reveals gap: leader commitIndex=%d, my commitIndex=%d (gap=%d) - triggering repair%n",
                    st.nodeNumericId, leaderCommitIndex, myCommitIndex, gap);


            final long target = leaderCommitIndex;
            final long startFrom = st.commitIndex;
            catchupExecutor.execute(() -> repairUpTo(startFrom, target));
        }

        mHeartbeatsRecv.incrementAndGet();
        resp.onNext(Ack.newBuilder().setMsg("").build());
        resp.onCompleted();
    }


    @Override
    public void twoPcPrepare(TwoPcPrepareMsg req, StreamObserver<TwoPcPreparedReply> resp) {
        if (!ensureActive(resp)) {
            return;
        }

        if (st.role != Roles.LEADER) {
            String leaderAddress = st.addressForNode(st.currentLeaderBallot.getLeaderId()).orElse("");
            if (leaderAddress.isEmpty()) {
                leaderAddress = st.addressForNode(st.highestSeenBallot.getLeaderId()).orElse("");
            }
            if (leaderAddress.isEmpty()) {
                if (electionInProgress.compareAndSet(false, true)) {
                    scheduler.execute(() -> {
                        try {
                            startElectionWithRetries();
                        } finally {
                            electionInProgress.set(false);
                        }
                    });
                }
            }
            TwoPcPreparedReply reply = TwoPcPreparedReply.newBuilder()
                    .setTxnId(req.getTxnId())
                    .setPrepared(false)
                    .setError("NOT_LEADER")
                    .setRedirect(leaderAddress)
                    .build();
            resp.onNext(reply);
            resp.onCompleted();
            return;
        }

        String txnId = req.getTxnId();
        Transaction tx = req.hasTx() ? req.getTx() : Transaction.getDefaultInstance();
        Integer receiverId = parseIntOrNull(tx.getReceiver());

        if (receiverId == null || !isLocalShardId(receiverId)) {
            TwoPcPreparedReply reply = TwoPcPreparedReply.newBuilder()
                    .setTxnId(txnId)
                    .setPrepared(false)
                    .setError("WRONG_SHARD")
                    .build();
            resp.onNext(reply);
            resp.onCompleted();
            return;
        }


        TwoPcManager.TxnState existing = st.twoPc.get(txnId).orElse(null);
        if (existing != null) {
            switch (existing.stage) {
                case PREPARED_LOCAL, PREPARED_REMOTE, COMMIT_DECIDED, COMMITTED -> {
                    TwoPcPreparedReply reply = TwoPcPreparedReply.newBuilder()
                            .setTxnId(txnId)
                            .setPrepared(true)
                            .setError("")
                            .build();
                    resp.onNext(reply);
                    resp.onCompleted();
                    return;
                }
                case ABORT_DECIDED, ABORTED -> {
                    TwoPcPreparedReply reply = TwoPcPreparedReply.newBuilder()
                            .setTxnId(txnId)
                            .setPrepared(false)
                            .setError("ABORTED")
                            .build();
                    resp.onNext(reply);
                    resp.onCompleted();
                    return;
                }
                default -> {}
            }
        }


        String recvKey = tx.getReceiver();
        boolean acquired = tryAcquireSingleLockWithTimer(recvKey);
        if (!acquired) {
            TwoPcPreparedReply reply = TwoPcPreparedReply.newBuilder()
                    .setTxnId(txnId)
                    .setPrepared(false)
                    .setError("LOCKED")
                    .build();
            resp.onNext(reply);
            resp.onCompleted();
            return;
        }

        st.twoPc.beginParticipant(txnId, tx, req.getDigest());

        ClientMeta meta = metaFromTxnId(txnId);
        Request paxosReq = Request.newBuilder()
                .setClient(ClientId.newBuilder().setId(meta.getClientId()))
                .setTs(Timestamp.newBuilder().setT(meta.getTimestamp()))
                .setTrans(tx)
                .build();

        OptionalLong seq = replicateAsLeaderWithStage(paxosReq, TwoPcStage.TWO_PC_PREPARE);
        if (seq.isEmpty()) {
            st.releaseSingleLock(recvKey);
            TwoPcPreparedReply reply = TwoPcPreparedReply.newBuilder()
                    .setTxnId(txnId)
                    .setPrepared(false)
                    .setError("REPLICATION_FAILED")
                    .build();
            resp.onNext(reply);
            resp.onCompleted();
            return;
        }
        st.twoPc.markPreparedLocal(txnId, seq.getAsLong());
        NodeState.ExecutionInfo info = waitForExecution(seq.getAsLong(), EXECUTION_WAIT);
        if (info == null || !info.success()) {
            st.releaseSingleLock(recvKey);
            TwoPcPreparedReply reply = TwoPcPreparedReply.newBuilder()
                    .setTxnId(txnId)
                    .setPrepared(false)
                    .setError(info == null ? "EXECUTION_TIMEOUT" : (info.error() == null ? "EXECUTION_FAILED" : info.error()))
                    .build();
            resp.onNext(reply);
            resp.onCompleted();
            return;
        }

        TwoPcPreparedReply reply = TwoPcPreparedReply.newBuilder()
                .setTxnId(txnId)
                .setPrepared(true)
                .setError("")
                .build();
        resp.onNext(reply);
        resp.onCompleted();
    }

    @Override
    public void twoPcCommit(TwoPcCommitMsg req, StreamObserver<Ack> resp) {
        if (!ensureActive(resp)) {
            return;
        }
        if (st.role != Roles.LEADER) {
            String leaderAddress = st.addressForNode(st.currentLeaderBallot.getLeaderId()).orElse("");
            String msg = leaderAddress.isEmpty() ? "NOT_LEADER" : ("REDIRECT " + leaderAddress);
            resp.onNext(Ack.newBuilder().setMsg(msg).build());
            resp.onCompleted();
            return;
        }
        String txnId = req.getTxnId();

        TwoPcManager.TxnState stx = st.twoPc.get(txnId).orElse(null);
        if (stx != null) {
            switch (stx.stage) {
                case COMMITTED, COMMIT_DECIDED -> {
                    resp.onNext(Ack.newBuilder().setMsg("").build());
                    resp.onCompleted();
                    return;
                }
                case ABORTED, ABORT_DECIDED -> {

                    resp.onNext(Ack.newBuilder().setMsg("ABORTED").build());
                    resp.onCompleted();
                    return;
                }
                default -> {}
            }
        }
        Transaction tx = st.twoPc.get(txnId).map(s -> s.tx).orElse(Transaction.getDefaultInstance());
        ClientMeta meta = metaFromTxnId(txnId);
        Request paxosReq = Request.newBuilder()
                .setClient(ClientId.newBuilder().setId(meta.getClientId()))
                .setTs(Timestamp.newBuilder().setT(meta.getTimestamp()))
                .setTrans(tx)
                .build();
        st.twoPc.decideCommit(txnId);
        OptionalLong seq = replicateAsLeaderWithStage(paxosReq, TwoPcStage.TWO_PC_COMMIT);
        if (seq.isPresent()) {
            waitForExecution(seq.getAsLong(), EXECUTION_WAIT);
            st.twoPc.markCommitted(txnId, seq.getAsLong());
        }
        resp.onNext(Ack.newBuilder().setMsg("").build());
        resp.onCompleted();
    }

    @Override
    public void twoPcAbort(TwoPcAbortMsg req, StreamObserver<Ack> resp) {
        if (!ensureActive(resp)) {
            return;
        }
        if (st.role != Roles.LEADER) {
            String leaderAddress = st.addressForNode(st.currentLeaderBallot.getLeaderId()).orElse("");
            String msg = leaderAddress.isEmpty() ? "NOT_LEADER" : ("REDIRECT " + leaderAddress);
            resp.onNext(Ack.newBuilder().setMsg(msg).build());
            resp.onCompleted();
            return;
        }
        String txnId = req.getTxnId();

        TwoPcManager.TxnState stx = st.twoPc.get(txnId).orElse(null);
        if (stx != null) {
            switch (stx.stage) {
                case ABORTED, ABORT_DECIDED -> {
                    resp.onNext(Ack.newBuilder().setMsg("").build());
                    resp.onCompleted();
                    return;
                }
                case COMMITTED, COMMIT_DECIDED -> {

                    resp.onNext(Ack.newBuilder().setMsg("COMMITTED").build());
                    resp.onCompleted();
                    return;
                }
                default -> {}
            }
        }
        Transaction tx = st.twoPc.get(txnId).map(s -> s.tx).orElse(Transaction.getDefaultInstance());
        ClientMeta meta = metaFromTxnId(txnId);
        Request paxosReq = Request.newBuilder()
                .setClient(ClientId.newBuilder().setId(meta.getClientId()))
                .setTs(Timestamp.newBuilder().setT(meta.getTimestamp()))
                .setTrans(tx)
                .build();
        st.twoPc.decideAbort(txnId);
        OptionalLong seq = replicateAsLeaderWithStage(paxosReq, TwoPcStage.TWO_PC_ABORT);
        if (seq.isPresent()) {
            waitForExecution(seq.getAsLong(), EXECUTION_WAIT);
            st.twoPc.markAborted(txnId, seq.getAsLong());
        }
        resp.onNext(Ack.newBuilder().setMsg("").build());
        resp.onCompleted();
    }

    /**
     * Attempt to repair log repeatedly until our commitIndex reaches targetCommitIndex,
     * or attempts are exhausted.
     */
    private void repairUpTo(long fromExclusive, long targetCommitIndex) {
        int attempts = 0;
        long cursor = Math.max(0, fromExclusive);
        while (attempts < 5) {
            attempts++;
            repairLog(cursor);
            long current;
            st.rw.readLock().lock();
            try {
                current = st.commitIndex;
            } finally {
                st.rw.readLock().unlock();
            }
            if (current >= targetCommitIndex) {
                break;
            }
            try {
                Thread.sleep(CATCHUP_RETRY_DELAY_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            st.rw.readLock().lock();
            try {
                cursor = Math.max(cursor, st.executedIndex);
            } finally {
                st.rw.readLock().unlock();
            }
        }
    }

    @Override
    public void newView(NewViewMsg req, StreamObserver<NewViewAck> resp) {
        if (!ensureActive(resp)) {
            return;
        }

        String fromId = req.hasFrom() ? req.getFrom().getId() : "?";
        System.out.printf("[node %d] NEWVIEW rpc from=%s ballot=(round=%d,leader=%d) entries=%d%n",
                st.nodeNumericId,
                fromId,
                req.getB().getRound(),
                req.getB().getLeaderId(),
                req.getEntriesCount());

        processNewView(req, false);

        NewViewAck ack = NewViewAck.newBuilder()
                .setB(req.getB())
                .setLastApplied(SeqNo.newBuilder().setValue(st.executedIndex))
                .setFrom(NodeId.newBuilder().setId(String.valueOf(st.nodeNumericId)))
                .build();
        resp.onNext(ack);
        resp.onCompleted();
    }

    @Override
    public void commit(CommitMsg req, StreamObserver<Ack> resp) {
        if (!ensureActive(resp)) {
            return;
        }

        NodeState.ExecutionInfo info = commitLocal(req);
        if (info != null) {
            notifyExecution(info);
        }

        // Check for gaps and trigger repair if needed
        st.rw.readLock().lock();
        try {
            if (st.executedIndex + 1 <= st.commitIndex) {
                long gapStart = st.executedIndex;
                System.out.printf("[node %d] Gap detected: execIndex=%d < commitIndex=%d, triggering repair%n",
                        st.nodeNumericId, st.executedIndex, st.commitIndex);
                catchupExecutor.execute(() -> repairLog(gapStart));
            }
        } finally {
            st.rw.readLock().unlock();
        }
        logCommitState("remote seq=" + req.getS().getValue());
        resp.onNext(Ack.newBuilder().setMsg("").build());
        resp.onCompleted();
    }

    @Override
    public void control(NodeControl req, StreamObserver<Ack> resp) {
        if (req.getActive()) {
            activate();
        } else {

            if (req.getIsLeaderFailure()) {
                if (active.get() && st.role == Roles.LEADER) {
                    scheduleLeaderFailureDeactivation(1000);
                } else {
                    deactivateForLeaderFailure();
                }
            } else {
                deactivate();
            }
        }
        resp.onNext(Ack.newBuilder().setMsg("").build());
        resp.onCompleted();
    }

    @Override
    public void catchup(CatchupRequest req, StreamObserver<CatchupReply> resp) {
        if (!ensureActive(resp)) {
            return;
        }
        int myClusterIdx = (st.nodeNumericId - 1) / 3;
        String requesterStr = req.getRequester().getId();
        Integer requesterId = parseIntOrNull(requesterStr);
        if (requesterId != null) {
            int requesterClusterIdx = (requesterId - 1) / 3;
            if (requesterClusterIdx != myClusterIdx) {
                resp.onNext(CatchupReply.newBuilder().build());
                resp.onCompleted();
                return;
            }
        }

        long from = req.hasFrom() ? req.getFrom().getValue() : 0L;
        CatchupReply.Builder reply = CatchupReply.newBuilder();

        st.rw.readLock().lock();
        try {
            long start = Math.max(1, from + 1);
            for (long seq = start; seq <= st.commitIndex; seq++) {
                Optional<Request> mOpt = st.requestFor(seq);
                if (mOpt.isEmpty()) {
                    break;
                }

                Ballot ballot = ballotFor(seq, st.currentLeaderBallot);
                TwoPcStage stage = TwoPcStage.TWO_PC_NONE;
                LogEntry le = st.log.get(seq);
                if (le != null) stage = le.getTwoPcStage();
                reply.addCommits(CommitMsg.newBuilder()
                        .setB(ballot)
                        .setS(SeqNo.newBuilder().setValue(seq))
                        .setM(mOpt.get())
                        .setStage(stage)
                        .build());
            }
        } finally {
            st.rw.readLock().unlock();
        }

        resp.onNext(reply.build());
        resp.onCompleted();
    }

    @Override
    public void getLocalSnapshot(com.google.protobuf.Empty req, StreamObserver<SnapshotOffer> resp) {
        if (!ensureActive(resp)) {
            return;
        }
        long executed;
        java.util.Map<Integer, Long> snapshot;
        st.rw.readLock().lock();
        try {
            executed = st.executedIndex;
            snapshot = st.accountStore.readAllBalances();
        } finally {
            st.rw.readLock().unlock();
        }
        SnapshotOffer.Builder b = SnapshotOffer.newBuilder().setLastApplied(executed);
        for (java.util.Map.Entry<Integer, Long> e : snapshot.entrySet()) {
            b.addEntries(SnapshotEntry.newBuilder().setId(e.getKey()).setBalance(e.getValue()));
        }
        resp.onNext(b.build());
        resp.onCompleted();
    }

    @Override
    public void installLocalSnapshot(SnapshotOffer req, StreamObserver<Ack> resp) {
        if (!ensureActive(resp)) {
            return;
        }
        long applied = req.getLastApplied();
        java.util.Map<Integer, Long> map = new java.util.LinkedHashMap<>();
        for (SnapshotEntry se : req.getEntriesList()) {
            map.put(se.getId(), se.getBalance());
        }
        st.rw.writeLock().lock();
        try {
            st.accountStore.replaceAllBalances(map);
            long oldExec = st.executedIndex;
            st.executedIndex = Math.max(st.executedIndex, applied);
            st.commitIndex = Math.max(st.commitIndex, st.executedIndex);
            try {
                if (st.executedIndex > 0) {
                    st.log.headMap(st.executedIndex, true).clear();
                }
            } catch (Exception ignore) {}
            st.nextSeq.set(Math.max(st.nextSeq.get(), st.executedIndex + 1));
            System.out.printf("[node %d] Installed snapshot: lastApplied=%d (was %d)\n", st.nodeNumericId, st.executedIndex, oldExec);
        } finally {
            st.rw.writeLock().unlock();
        }
        resp.onNext(Ack.newBuilder().setMsg("").build());
        resp.onCompleted();
    }
    // used the help of chatGPT to generate the below functions - printView, printStatus, printLog, printDB
    @Override
    public void printView(Empty req, StreamObserver<PrintViewDump> resp) {
        PrintViewDump dump;
        st.rw.readLock().lock();
        try {
            dump = PrintViewDump.newBuilder()
                    .addAllItems(st.viewHistorySnapshot())
                    .build();
        } finally {
            st.rw.readLock().unlock();
        }
        resp.onNext(dump);
        resp.onCompleted();
    }

    @Override
    public void printStatus(StatusRequest req, StreamObserver<StatusDump> resp) {
        long seq = req.hasS() ? req.getS().getValue() : 0L;
        StatusDump dump;
        st.rw.readLock().lock();
        try {
            NodeState.Phase phase = st.phaseFor(seq);
            StatusEntry entry = StatusEntry.newBuilder()
                    .setNode(NodeId.newBuilder().setId(String.valueOf(st.nodeNumericId)))
                    .setPhase(String.valueOf(phase.symbol()))
                    .build();
            dump = StatusDump.newBuilder().addEntries(entry).build();
        } finally {
            st.rw.readLock().unlock();
        }
        resp.onNext(dump);
        resp.onCompleted();
    }

    @Override
    public void printLog(Empty req, StreamObserver<LogDump> resp) {
        String logContent;
        st.rw.readLock().lock();
        try {
            logContent = st.formatLog();
        } finally {
            st.rw.readLock().unlock();
        }
        LogDump dump = LogDump.newBuilder()
                .setNodeId(st.nodeNumericId)
                .setContent(logContent)
                .build();
        resp.onNext(dump);
        resp.onCompleted();
    }

    @Override
    public void printDB(Empty req, StreamObserver<DBDump> resp) {
        String dbContent;
        st.rw.readLock().lock();
        try {
            dbContent = st.formatDb();
        } finally {
            st.rw.readLock().unlock();
        }
        DBDump dump = DBDump.newBuilder()
                .setNodeId(st.nodeNumericId)
                .setContent(dbContent)
                .build();
        resp.onNext(dump);
        resp.onCompleted();
    }

    @Override
    public void getBalance(BalanceRequest req, StreamObserver<BalanceReply> resp) {
        if (!ensureActive(resp)) {
            return;
        }
        if (st.role != Roles.LEADER) {
            resp.onError(io.grpc.Status.FAILED_PRECONDITION
                    .withDescription("NOT_LEADER")
                    .asRuntimeException());
            return;
        }
        int id = req.getId();
        long bal = st.readBalance(id);
        BalanceReply reply = BalanceReply.newBuilder()
                .setNodeId(st.nodeNumericId)
                .setId(id)
                .setBalance(bal)
                .build();
        resp.onNext(reply);
        resp.onCompleted();
    }

    @Override
    public void reset(Empty req, StreamObserver<Ack> resp) {
        cancelDeferredPrepareTask();
        deferredPrepare.set(null);

        st.rw.writeLock().lock();
        try {
            st.resetAllForNewSet();
            st.accountStore.reset();
            st.role = Roles.FOLLOWER;
            st.highestSeenBallot = Ballot.newBuilder().setRound(0).setLeaderId(0).build();
            st.currentLeaderBallot = Ballot.newBuilder().setRound(0).setLeaderId(0).build();
            long now = System.currentTimeMillis();
            st.lastHeartbeatSeenMs = now;
            st.lastPrepareSeenMs = now;
        } finally {
            st.rw.writeLock().unlock();
        }

        catchupChannels.values().forEach(ch -> { try { ch.shutdownNow(); } catch (Exception ignore) {} });
        catchupChannels.clear();
        catchupStubCache.clear();
        remoteRpcChannels.values().forEach(ch -> { try { ch.shutdownNow(); } catch (Exception ignore) {} });
        remoteRpcChannels.clear();
        remoteRpcStubCache.clear();
        resp.onNext(Ack.newBuilder().setMsg("").build());
        resp.onCompleted();
    }

    private static Integer parseIntOrNull(String s) {
        if (s == null) return null;
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException ex) {
            return null;
        }
    }


    private List<PromiseMsg> broadcastPrepare(Ballot myB) {
        PrepareMsg req = PrepareMsg.newBuilder()
                .setB(myB)
                .setFrom(NodeId.newBuilder().setId(String.valueOf(st.nodeNumericId)))
                .build();

        List<PromiseMsg> replies = new ArrayList<>();
        if (!active.get()) {
            return replies;
        }

        for (NodeServiceGrpc.NodeServiceBlockingStub stub : peerStubs) {
            try {
                PromiseMsg p = stub.prepare(req);
                replies.add(p);
                String from = p.hasFrom() ? p.getFrom().getId() : "?";
                StringBuilder logSummary = new StringBuilder();
                for (AcceptTriplet triplet : p.getAcceptLogList()) {
                    logSummary.append(String.format(" #%d(b=%d,m=%s)",
                            triplet.getS().getValue(),
                            triplet.getB().getRound(),
                            pretty(triplet.getM())));
                }
                System.out.printf("[node %d] <- PROMISE from %s (b=%d, highest_seen=%d) log:%s%n",
                        st.nodeNumericId,
                        from,
                        p.getB().getRound(),
                        p.hasHighestSeen() ? p.getHighestSeen().getRound() : -1,
                        logSummary);
            } catch (Exception ignored) {
            }
        }
        return replies;
    }

    public boolean startElectionAndMaybeWin() {
        if (!active.get()) {
            return false;
        }

        Ballot myB = st.nextBallot();
        System.out.printf("[node %d] Prepare(b=%d) -> broadcasting...%n",
                st.nodeNumericId, myB.getRound());

        st.rw.writeLock().lock();
        try {
            st.highestSeenBallot = myB;
            st.role = Roles.CANDIDATE;
        } finally {
            st.rw.writeLock().unlock();
        }

        List<PromiseMsg> acks = broadcastPrepare(myB);

        Ballot highestSeenOverall = st.highestSeenBallot;
        for (PromiseMsg pm : acks) {
            if (pm.hasHighestSeen() && greater(pm.getHighestSeen(), highestSeenOverall)) {
                highestSeenOverall = pm.getHighestSeen();
            }
        }

        int votes = 1;
        List<String> granted = new ArrayList<>();
        List<PromiseMsg> promises = new ArrayList<>();
        for (PromiseMsg p : acks) {

            if (greater(p.getB(), myB)) {
                st.rw.writeLock().lock();
                try {
                    st.role = Roles.FOLLOWER;
                    st.highestSeenBallot = p.getB();
                } finally {
                    st.rw.writeLock().unlock();
                }
                st.lastHeartbeatSeenMs = System.currentTimeMillis();
                System.out.printf("[node %d] saw higher ballot=%d; demoting%n",
                        st.nodeNumericId, p.getB().getRound());
                return false;
            }


            if (p.getB().getRound() == myB.getRound() && p.getB().getLeaderId() == myB.getLeaderId()) {
                votes++;
                promises.add(p);
                granted.add(p.hasFrom() ? p.getFrom().getId() : "?");
            }
        }

        boolean won = st.isQuorum(votes);
        if (won) {
            st.rw.writeLock().lock();
            try {
                st.role = Roles.LEADER;
                st.currentLeaderBallot = myB;
                if (greater(highestSeenOverall, st.highestSeenBallot)) {
                    st.highestSeenBallot = highestSeenOverall;
                }
                st.lastHeartbeatSeenMs = System.currentTimeMillis();
            } finally {
                st.rw.writeLock().unlock();
            }

            System.out.printf(
                    "[node %d] MAJORITY for Prepare(b=%d): %d/%d (self + %s). Becoming LEADER.%n",
                    st.nodeNumericId, myB.getRound(), votes, st.clusterSize, String.join(",", granted)
            );

            flushDeferredPrepare();

            TreeMap<Long, AcceptTriplet> merged = new TreeMap<>();
            for (PromiseMsg promise : promises) {
                for (AcceptTriplet triplet : promise.getAcceptLogList()) {
                    long seq = triplet.getS().getValue();
                    merged.compute(seq, (k, existing) ->
                            existing == null || greater(triplet.getB(), existing.getB()) ? triplet : existing);
                }
            }

            st.rw.readLock().lock();
            try {
                List<AcceptTriplet> ownAccepted = st.getAcceptedEntries();
                for (AcceptTriplet triplet : ownAccepted) {
                    long seq = triplet.getS().getValue();
                    merged.compute(seq, (k, existing) ->
                            existing == null || greater(triplet.getB(), existing.getB()) ? triplet : existing);
                }
            } finally {
                st.rw.readLock().unlock();
            }

            long maxRecovered = merged.isEmpty() ? st.executedIndex : merged.lastKey();
            List<AcceptEntry> entries = new ArrayList<>();

            if (!merged.isEmpty()) {
                for (long seq = 1; seq <= maxRecovered; seq++) {
                    AcceptEntry.Builder builder = AcceptEntry.newBuilder()
                            .setS(SeqNo.newBuilder().setValue(seq));

                    TwoPcStage stage = TwoPcStage.TWO_PC_NONE;
                    LogEntry le = st.log.get(seq);
                    if (le != null) stage = le.getTwoPcStage();
                    AcceptTriplet triplet = merged.get(seq);
                    if (triplet != null) {
                        boolean noop = NodeState.isNoop(triplet.getM());
                        builder.setSrcBallot(myB)
                                .setM(noop ? NodeState.NOOP : triplet.getM())
                                .setIsNoop(noop)
                                .setStage(stage);
                    } else {
                        builder.setSrcBallot(myB)
                                .setM(NodeState.NOOP)
                                .setIsNoop(true)
                                .setStage(stage);
                    }
                    entries.add(builder.build());
                }
            } else {
                st.rw.writeLock().lock();
                try {
                    long safeNext = Math.max(st.executedIndex, st.commitIndex) + 1;
                    if (st.nextSeq.get() < safeNext) {
                        st.nextSeq.set(safeNext);
                        System.out.printf("[node %d] No recovery, set nextSeq=%d (exec=%d, commit=%d)%n",
                                st.nodeNumericId, safeNext, st.executedIndex, st.commitIndex);
                    }
                } finally {
                    st.rw.writeLock().unlock();
                }
            }

            NewViewMsg newView = NewViewMsg.newBuilder()
                    .setB(myB)
                    .setFrom(NodeId.newBuilder().setId(String.valueOf(st.nodeNumericId)))
                    .addAllEntries(entries)
                    .build();

            System.out.printf("[node %d] Broadcasting newView with %d entries (seq 1-%d)%n",
                    st.nodeNumericId, entries.size(),
                    entries.isEmpty() ? 0 : entries.get(entries.size() - 1).getS().getValue());

            List<NewViewAck> newViewAcks = broadcastNewView(newView);
            processNewView(newView, true);

            int totalAcks = 1 + newViewAcks.size();
            boolean hasQuorum = st.isQuorum(totalAcks);

            System.out.printf("[node %d] NEW-VIEW quorum check: %d/%d acks (quorum=%s)%n",
                    st.nodeNumericId, totalAcks, st.clusterSize, hasQuorum);

            if (!hasQuorum) {
                System.out.printf("[node %d] WARNING: NEW-VIEW got %d/%d acks (below quorum)",
                        st.nodeNumericId, totalAcks, st.clusterSize);
            }
            st.rw.writeLock().lock();
            try {
                System.out.printf("[node %d] Before catch-up: execIndex=%d, commitIndex=%d, log size=%d%n",
                        st.nodeNumericId, st.executedIndex, st.commitIndex, st.log.size());

                for (AcceptEntry entry : entries) {
                    long seq = entry.getS().getValue();
                    st.markCommitted(seq);
                    if (seq > st.commitIndex) {
                        st.commitIndex = seq;
                    }
                }

                while (st.executedIndex < st.commitIndex) {
                    long nextExec = st.executedIndex + 1;
                    Optional<Request> reqOpt = st.requestFor(nextExec);
                    if (reqOpt.isEmpty()) {
                        System.out.printf("[node %d] CRITICAL ERROR: Gap at seq=%d after newView (commitIndex=%d)%n",
                                st.nodeNumericId, nextExec, st.commitIndex);
                        break;
                    }
                    Request req = reqOpt.get();
                    Ballot execBallot = ballotFor(nextExec, myB);
                    NodeState.ExecutionInfo info = execute(nextExec, req, execBallot);

                    notifyExecution(info);

                    TwoPcStage stageNow = TwoPcStage.TWO_PC_NONE;
                    LogEntry leNow = st.log.get(nextExec);
                    if (leNow != null) stageNow = leNow.getTwoPcStage();
                    boolean terminal = (stageNow == TwoPcStage.TWO_PC_NONE) ||
                            (stageNow == TwoPcStage.TWO_PC_COMMIT) ||
                            (stageNow == TwoPcStage.TWO_PC_ABORT);
                    if (terminal) {
                        st.executedIndex = nextExec;
                    } else {
                        break;
                    }
                    System.out.printf("[node %d] Executed recovered seq=%d during leader election%n",
                            st.nodeNumericId, nextExec);
                }

                long maxInLog = st.log.isEmpty() ? 0L : st.log.lastKey();
                long safeNext = Math.max(st.executedIndex + 1,
                        Math.max(st.commitIndex + 1, maxInLog + 1));
                if (!entries.isEmpty()) {
                    long maxFromNewView = entries.get(entries.size() - 1).getS().getValue() + 1;
                    safeNext = Math.max(safeNext, maxFromNewView);
                }
                st.nextSeq.set(safeNext);

                System.out.printf("[node %d] Leader ready after recovery: execIndex=%d, commitIndex=%d, maxInLog=%d, nextSeq=%d%n",
                        st.nodeNumericId, st.executedIndex, st.commitIndex, maxInLog, safeNext);
            } finally {
                st.rw.writeLock().unlock();
            }

            for (AcceptEntry entry : entries) {
                long seq = entry.getS().getValue();
                Request request = entry.getIsNoop() ? NodeState.NOOP : entry.getM();

                CommitMsg cm = CommitMsg.newBuilder()
                        .setB(myB)
                        .setS(entry.getS())
                        .setM(request)
                        .setStage(entry.getStage())
                        .build();

                peerStubs.forEach(stub -> {
                    try {
                        stub.commit(cm);
                    } catch (Exception ignored) {}
                });
            }

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            if (broadcastHeartbeat()) {
                lastHeartbeatBroadcastMs = System.currentTimeMillis();
            }
        } else {
            st.rw.writeLock().lock();
            try {
                st.role = Roles.FOLLOWER;
            } finally {
                st.rw.writeLock().unlock();
            }
            st.lastHeartbeatSeenMs = System.currentTimeMillis();
            System.out.printf("[node %d] No majority (got %d/%d). Staying FOLLOWER.%n",
                    st.nodeNumericId, votes, st.clusterSize);
        }
        return won;
    }
    // used chatGPT to generate this code
    private boolean startElectionWithRetries() {
        int maxRetries = 3;

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            if (!active.get()) {
                return false;
            }

            System.out.printf("[node %d] Election attempt %d/%d%n",
                    st.nodeNumericId, attempt + 1, maxRetries);

            boolean won = startElectionAndMaybeWin();
            if (won) {
                return true;
            }


            if (attempt < maxRetries - 1) {
                try {
                    long backoffMs = 100 * (1 << attempt);
                    System.out.printf("[node %d] Election failed, retrying in %d ms%n",
                            st.nodeNumericId, backoffMs);
                    Thread.sleep(backoffMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        System.out.printf("[node %d] Election failed after %d attempts%n",
                st.nodeNumericId, maxRetries);
        return false;
    }

    private void applySnapshotOfferLocal(SnapshotOffer offer) {
        java.util.Map<Integer, Long> map = new java.util.LinkedHashMap<>();
        for (SnapshotEntry se : offer.getEntriesList()) {
            map.put(se.getId(), se.getBalance());
        }
        st.rw.writeLock().lock();
        try {
            st.accountStore.replaceAllBalances(map);
            long applied = offer.getLastApplied();
            st.executedIndex = Math.max(st.executedIndex, applied);
            st.commitIndex = Math.max(st.commitIndex, st.executedIndex);
            try {
                if (st.executedIndex > 0) {
                    st.log.headMap(st.executedIndex, true).clear();
                }
            } catch (Exception ignore) {}
            st.nextSeq.set(Math.max(st.nextSeq.get(), st.executedIndex + 1));
            System.out.printf("[node %d] Applied snapshot offer: lastApplied=%d\n", st.nodeNumericId, st.executedIndex);
        } finally {
            st.rw.writeLock().unlock();
        }
    }

    public OptionalLong replicateAsLeader(Request req) {
        if (!active.get()) {
            throw new IllegalStateException("Node inactive");
        }
        if (st.role != Roles.LEADER) {
            throw new IllegalStateException("Not leader");
        }
        ClientMeta meta = extractMetadata(req);
        if (meta != null) {
            Long existing = findExistingSeq(meta.getClientId(), meta.getTimestamp());
            if (existing != null) {
                Request existingReq;
                st.rw.readLock().lock();
                try {
                    existingReq = st.requestFor(existing).orElse(NodeState.NOOP);
                } finally {
                    st.rw.readLock().unlock();
                }

                TwoPcStage stage = TwoPcStage.TWO_PC_NONE;
                st.rw.readLock().lock();
                try {
                    LogEntry le = st.log.get(existing);
                    if (le != null) stage = le.getTwoPcStage();
                } finally {
                    st.rw.readLock().unlock();
                }
                CommitMsg cm = CommitMsg.newBuilder()
                        .setB(st.currentLeaderBallot)
                        .setS(SeqNo.newBuilder().setValue(existing))
                        .setM(existingReq)
                        .setStage(stage)
                        .build();
                if (PAXOS_ASYNC_RPC) {
                    broadcastCommit(cm);
                } else {
                    for (NodeServiceGrpc.NodeServiceBlockingStub stub : peerStubs) {
                        try {
                            mCommitSent.incrementAndGet();
                            NodeServiceGrpc.NodeServiceBlockingStub callStub = maybeCompressStub(stub, cm.getSerializedSize())
                                    .withDeadlineAfter(FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                            callStub.commit(cm);
                        } catch (Exception ignored) {
                            mRpcErrors.incrementAndGet();
                        }
                    }
                }

                NodeState.ExecutionInfo info = commitLocal(cm);
                if (info != null) {
                    notifyExecution(info);
                }
                logCommitState("local seq=" + existing);
                return OptionalLong.of(existing);
            }
        }

        boolean acquired = false;
        if (PAXOS_PIPELINING) {
            pipelineSem.acquireUninterruptibly();
            acquired = true;
        }

        long seq = st.nextSeq.getAndIncrement();
        AcceptMsg am = AcceptMsg.newBuilder()
                .setB(st.currentLeaderBallot)
                .setS(SeqNo.newBuilder().setValue(seq))
                .setM(req)
                .build();

        st.rw.writeLock().lock();
        try {
            stageAccept(am);
        } finally {
            st.rw.writeLock().unlock();
        }

        if (PAXOS_VERBOSE_LOGS) {
            System.out.printf("[node %d] -> ACCEPT s=%d value=%s%n",
                    st.nodeNumericId, seq, pretty(req));
        }

        AtomicReference<Ballot> highestSeenFromPeersRef = new AtomicReference<>(st.currentLeaderBallot);
        boolean hasQuorum = sendAcceptAndAwaitQuorum(am, highestSeenFromPeersRef);
        if (!hasQuorum) {
            System.out.printf("[node %d] No quorum for s=%d. Demoting.%n", st.nodeNumericId, seq);
            st.rw.writeLock().lock();
            try {
                st.role = Roles.FOLLOWER;
                st.highestSeenBallot = highestSeenFromPeersRef.get();
            } finally {
                st.rw.writeLock().unlock();
            }
            discardSpeculativeLocalAccept(seq);
            if (acquired) pipelineSem.release();
            failPendingWaiters("lost-quorum");
            return OptionalLong.empty();
        }


        TwoPcStage cmStage = TwoPcStage.TWO_PC_NONE;
        st.rw.readLock().lock();
        try {
            LogEntry le = st.log.get(seq);
            if (le != null) cmStage = le.getTwoPcStage();
        } finally {
            st.rw.readLock().unlock();
        }
        CommitMsg cm = CommitMsg.newBuilder()
                .setB(st.currentLeaderBallot)
                .setS(am.getS())
                .setM(am.getM())
                .setStage(cmStage)
                .build();

        if (PAXOS_ASYNC_RPC) {
            broadcastCommit(cm);
        } else {
            for (NodeServiceGrpc.NodeServiceBlockingStub stub : peerStubs) {
                try {
                    mCommitSent.incrementAndGet();
                    stub.withDeadlineAfter(FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS).commit(cm);
                } catch (Exception ignored) {
                    mRpcErrors.incrementAndGet();
                }
            }
        }

        NodeState.ExecutionInfo info = commitLocal(cm);
        if (info != null) {
            notifyExecution(info);
        }
        logCommitState("local seq=" + seq);
        if (acquired) pipelineSem.release();
        return OptionalLong.of(seq);
    }

    private OptionalLong replicateAsLeaderWithStage(Request req, TwoPcStage stage) {
        if (stage == TwoPcStage.TWO_PC_NONE) {
            return replicateAsLeader(req);
        }
        if (!active.get()) {
            throw new IllegalStateException("Node inactive");
        }
        if (st.role != Roles.LEADER) {
            throw new IllegalStateException("Not leader");
        }
        ClientMeta meta = extractMetadata(req);
        Long existing = null;
        TwoPcStage existingStage = TwoPcStage.TWO_PC_NONE;
        if (meta != null) {
            existing = findExistingSeq(meta.getClientId(), meta.getTimestamp());
            if (existing != null) {
                st.rw.readLock().lock();
                try {
                    LogEntry le = st.log.get(existing);
                    if (le != null) existingStage = le.getTwoPcStage();
                } finally {
                    st.rw.readLock().unlock();
                }

                if (existingStage == stage) {
                    Request existingReq;
                    st.rw.readLock().lock();
                    try {
                        existingReq = st.requestFor(existing).orElse(NodeState.NOOP);
                    } finally {
                        st.rw.readLock().unlock();
                    }
                    CommitMsg cm = CommitMsg.newBuilder()
                            .setB(st.currentLeaderBallot)
                            .setS(SeqNo.newBuilder().setValue(existing))
                            .setM(existingReq)
                            .setStage(existingStage)
                            .build();
                    if (PAXOS_ASYNC_RPC) {
                        broadcastCommit(cm);
                    } else {
                        for (NodeServiceGrpc.NodeServiceBlockingStub stub : peerStubs) {
                            try { mCommitSent.incrementAndGet(); stub.withDeadlineAfter(FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS).commit(cm); } catch (Exception ignored) { mRpcErrors.incrementAndGet(); }
                        }
                    }
                    NodeState.ExecutionInfo info = commitLocal(cm);
                    if (info != null) notifyExecution(info);
                    logCommitState("local seq=" + existing);
                    return OptionalLong.of(existing);
                }

                if ((stage == TwoPcStage.TWO_PC_COMMIT || stage == TwoPcStage.TWO_PC_ABORT)) {
                    CommitMsg cm = CommitMsg.newBuilder()
                            .setB(st.currentLeaderBallot)
                            .setS(SeqNo.newBuilder().setValue(existing))
                            .setM(req)
                            .setStage(stage)
                            .build();
                    if (PAXOS_ASYNC_RPC) {
                        broadcastCommit(cm);
                    } else {
                        for (NodeServiceGrpc.NodeServiceBlockingStub stub : peerStubs) {
                            try { mCommitSent.incrementAndGet(); stub.withDeadlineAfter(FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS).commit(cm); } catch (Exception ignored) { mRpcErrors.incrementAndGet(); }
                        }
                    }
                    NodeState.ExecutionInfo info = commitLocal(cm);
                    if (info != null) notifyExecution(info);
                    logCommitState("local seq=" + existing);
                    return OptionalLong.of(existing);
                }
            }
        }


        boolean acquired = false;
        if (PAXOS_PIPELINING) { pipelineSem.acquireUninterruptibly(); acquired = true; }
        long seq;
        if (stage == TwoPcStage.TWO_PC_PREPARE) {
            seq = (existing != null) ? existing : st.nextSeq.getAndIncrement();
        } else {

            seq = st.nextSeq.getAndIncrement();
        }

        AcceptMsg am = AcceptMsg.newBuilder()
                .setB(st.currentLeaderBallot)
                .setS(SeqNo.newBuilder().setValue(seq))
                .setM(req)
                .setStage(stage)
                .build();

        st.rw.writeLock().lock();
        try { stageAccept(am); } finally { st.rw.writeLock().unlock(); }

        int acks = 1;
        Ballot highestSeenFromPeers = st.currentLeaderBallot;
        if (PAXOS_VERBOSE_LOGS) {
            System.out.printf("[node %d] -> ACCEPT s=%d value=%s stage=%s%n",
                    st.nodeNumericId, seq, pretty(req), stage);
        }
        AtomicReference<Ballot> highestSeenFromPeersRef = new AtomicReference<>(highestSeenFromPeers);
        boolean hasQuorum = sendAcceptAndAwaitQuorum(am, highestSeenFromPeersRef);
        if (!hasQuorum) {
            st.rw.writeLock().lock();
            try { st.role = Roles.FOLLOWER; st.highestSeenBallot = highestSeenFromPeersRef.get(); }
            finally { st.rw.writeLock().unlock(); }
            discardSpeculativeLocalAccept(seq);
            if (acquired) pipelineSem.release();
            failPendingWaiters("lost-quorum");
            return OptionalLong.empty();
        }
        CommitMsg cm = CommitMsg.newBuilder()
                .setB(st.currentLeaderBallot)
                .setS(am.getS())
                .setM(am.getM())
                .setStage(stage)
                .build();
        if (PAXOS_ASYNC_RPC) {
            broadcastCommit(cm);
        } else {
            for (NodeServiceGrpc.NodeServiceBlockingStub stub : peerStubs) {
                try { mCommitSent.incrementAndGet(); stub.withDeadlineAfter(FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS).commit(cm); } catch (Exception ignored) { mRpcErrors.incrementAndGet(); }
            }
        }
        NodeState.ExecutionInfo info = commitLocal(cm);
        if (info != null) notifyExecution(info);
        logCommitState("local seq=" + seq);
        if (acquired) pipelineSem.release();
        return OptionalLong.of(seq);
    }

    private boolean sendAcceptAndAwaitQuorum(AcceptMsg am, java.util.concurrent.atomic.AtomicReference<Ballot> highestSeenFromPeersRef) {
        if (!PAXOS_ASYNC_RPC) {
            int acks = 1;
            Ballot highest = highestSeenFromPeersRef.get();
            for (NodeServiceGrpc.NodeServiceBlockingStub stub : peerStubs) {
                try {
                    mAcceptSent.incrementAndGet();
                    NodeServiceGrpc.NodeServiceBlockingStub callStub = maybeCompressStub(stub, am.getSerializedSize())
                            .withDeadlineAfter(FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    AcceptedMsg r = callStub.accept(am);
                    if (greater(r.getB(), st.currentLeaderBallot)) {
                        if (greater(r.getB(), highest)) {
                            highest = r.getB();
                        }
                    } else {
                        mAcceptAcks.incrementAndGet();
                        acks++;
                    }
                } catch (Exception ignored) {
                    mRpcErrors.incrementAndGet();
                }
            }
            highestSeenFromPeersRef.set(highest);
            return st.isQuorum(acks);
        }

        final AtomicInteger ackCount = new AtomicInteger(1);
        final java.util.concurrent.CountDownLatch finish = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.CountDownLatch all = new java.util.concurrent.CountDownLatch(peerStubs.size());
        for (NodeServiceGrpc.NodeServiceBlockingStub stub : peerStubs) {
            replicationExecutor.submit(() -> {
                try {
                    mAcceptSent.incrementAndGet();
                    NodeServiceGrpc.NodeServiceBlockingStub callStub = maybeCompressStub(stub, am.getSerializedSize())
                            .withDeadlineAfter(FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    AcceptedMsg r = callStub.accept(am);
                    if (greater(r.getB(), st.currentLeaderBallot)) {

                        Ballot cur;
                        do { cur = highestSeenFromPeersRef.get(); } while (greater(r.getB(), cur) && !highestSeenFromPeersRef.compareAndSet(cur, r.getB()));
                    } else {
                        int now = ackCount.incrementAndGet();
                        mAcceptAcks.incrementAndGet();
                        if (st.isQuorum(now)) {
                            finish.countDown();
                        }
                    }
                } catch (Exception ignored) {
                    mRpcErrors.incrementAndGet();
                } finally {
                    all.countDown();
                }
            });
        }
        try {
            if (!finish.await(Math.max(1L, FORWARD_TIMEOUT_MS), TimeUnit.MILLISECONDS)) {

                all.await(Math.max(1L, FORWARD_TIMEOUT_MS), TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return st.isQuorum(ackCount.get());
    }

    private void broadcastCommit(CommitMsg cm) {
        for (NodeServiceGrpc.NodeServiceBlockingStub stub : peerStubs) {
            replicationExecutor.submit(() -> {
                try {
                    mCommitSent.incrementAndGet();
                    NodeServiceGrpc.NodeServiceBlockingStub callStub = maybeCompressStub(stub, cm.getSerializedSize())
                            .withDeadlineAfter(FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    callStub.commit(cm);
                } catch (Exception ignored) {
                    mRpcErrors.incrementAndGet();
                }
            });
        }
    }

    private NodeServiceGrpc.NodeServiceBlockingStub maybeCompressStub(NodeServiceGrpc.NodeServiceBlockingStub base, int serializedBytes) {
        String mode = PAXOS_RPC_COMPRESSION_MODE == null ? "off" : PAXOS_RPC_COMPRESSION_MODE.trim().toLowerCase();
        switch (mode) {
            case "always":
                return base.withCompression("gzip");
            case "auto":
                return (serializedBytes >= Math.max(0, PAXOS_RPC_COMPRESSION_THRESHOLD_BYTES)) ? base.withCompression("gzip") : base;
            default:
                return base;
        }
    }

    private Long findExistingSeq(String clientId, long ts) {
        st.rw.readLock().lock();
        try {
            for (Map.Entry<Long, LogEntry> e : st.log.entrySet()) {
                Request r = e.getValue().getRequest();
                ClientMeta m = extractMetadata(r);
                if (m != null && clientId.equals(m.getClientId()) && ts == m.getTimestamp()) {
                    return e.getKey();
                }
            }
        } finally {
            st.rw.readLock().unlock();
        }
        return null;
    }

    private ClientMeta metaFromTxnId(String txnId) {
        if (txnId == null || txnId.isBlank()) return ClientMeta.getDefaultInstance();
        int idx = txnId.lastIndexOf(':');
        if (idx <= 0 || idx >= txnId.length() - 1) return ClientMeta.getDefaultInstance();
        String client = txnId.substring(0, idx);
        try {
            long ts = Long.parseLong(txnId.substring(idx + 1));
            return ClientMeta.newBuilder().setClientId(client).setTimestamp(ts).build();
        } catch (NumberFormatException e) {
            return ClientMeta.getDefaultInstance();
        }
    }

    private boolean callParticipantPrepare(String txnId, Transaction tx, int participantCluster) {
        TwoPcPrepareMsg msg = TwoPcPrepareMsg.newBuilder()
                .setTxnId(txnId)
                .setTx(tx)
                .build();
        for (int attempt = 0; attempt < 10; attempt++) {
            String leaderRedirect = null;
            for (Integer id : st.numericIdsByCluster(participantCluster)) {
                NodeServiceGrpc.NodeServiceBlockingStub stub = rpcStubForNode(id);
                if (stub == null) continue;
                try {
                    NodeServiceGrpc.NodeServiceBlockingStub callStub = maybeCompressStub(stub, msg.getSerializedSize())
                            .withDeadlineAfter(FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    TwoPcPreparedReply r = callStub.twoPcPrepare(msg);
                    if (r.getPrepared()) return true;
                    if (!r.getRedirect().isBlank()) leaderRedirect = r.getRedirect();
                } catch (Exception ignored) {}
            }
            if (leaderRedirect != null && !leaderRedirect.isBlank()) {
                try {
                    ManagedChannel ch = NettyChannelBuilder.forTarget(leaderRedirect)
                            .usePlaintext()
                            .keepAliveTime(PAXOS_RPC_KEEPALIVE_TIME_SEC, TimeUnit.SECONDS)
                            .keepAliveTimeout(PAXOS_RPC_KEEPALIVE_TIMEOUT_SEC, TimeUnit.SECONDS)
                            .keepAliveWithoutCalls(PAXOS_RPC_KEEPALIVE_WITHOUT_CALLS)
                            .maxInboundMessageSize(PAXOS_RPC_MAX_INBOUND_MESSAGE_MB * 1024 * 1024)
                            .flowControlWindow(PAXOS_RPC_FLOW_CONTROL_WINDOW_BYTES)
                            .withOption(ChannelOption.TCP_NODELAY, PAXOS_RPC_TCP_NODELAY)
                            .build();
                    NodeServiceGrpc.NodeServiceBlockingStub stub2 = NodeServiceGrpc.newBlockingStub(ch);
                    NodeServiceGrpc.NodeServiceBlockingStub callStub = maybeCompressStub(stub2, msg.getSerializedSize())
                            .withDeadlineAfter(FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    TwoPcPreparedReply r = callStub.twoPcPrepare(msg);
                    ch.shutdownNow();
                    if (r.getPrepared()) return true;
                } catch (Exception ignored) {}
            }
            try { Thread.sleep(300); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
        return false;
    }

    private void callParticipantCommit(String txnId, int participantCluster) {
        TwoPcCommitMsg msg = TwoPcCommitMsg.newBuilder().setTxnId(txnId).build();
        for (int attempt = 0; attempt < 10; attempt++) {
            String redirect = null;
            boolean delivered = false;
            for (Integer id : st.numericIdsByCluster(participantCluster)) {
                NodeServiceGrpc.NodeServiceBlockingStub stub = rpcStubForNode(id);
                if (stub == null) continue;
                try {
                    NodeServiceGrpc.NodeServiceBlockingStub callStub = maybeCompressStub(stub, msg.getSerializedSize())
                            .withDeadlineAfter(FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    Ack ack = callStub.twoPcCommit(msg);
                    String m = ack.getMsg();
                    if (m != null && m.startsWith("REDIRECT ")) {
                        redirect = m.substring("REDIRECT ".length());
                        break;
                    }
                    if (m == null || m.isBlank()) {
                        return;
                    }

                } catch (Exception ignored) {}
            }
            if (redirect != null && !redirect.isBlank()) {
                try {
                    ManagedChannel ch = NettyChannelBuilder.forTarget(redirect)
                            .usePlaintext()
                            .keepAliveTime(PAXOS_RPC_KEEPALIVE_TIME_SEC, TimeUnit.SECONDS)
                            .keepAliveTimeout(PAXOS_RPC_KEEPALIVE_TIMEOUT_SEC, TimeUnit.SECONDS)
                            .keepAliveWithoutCalls(PAXOS_RPC_KEEPALIVE_WITHOUT_CALLS)
                            .maxInboundMessageSize(PAXOS_RPC_MAX_INBOUND_MESSAGE_MB * 1024 * 1024)
                            .flowControlWindow(PAXOS_RPC_FLOW_CONTROL_WINDOW_BYTES)
                            .withOption(ChannelOption.TCP_NODELAY, PAXOS_RPC_TCP_NODELAY)
                            .build();
                    NodeServiceGrpc.NodeServiceBlockingStub stub2 = NodeServiceGrpc.newBlockingStub(ch);
                    NodeServiceGrpc.NodeServiceBlockingStub callStub = maybeCompressStub(stub2, msg.getSerializedSize())
                            .withDeadlineAfter(FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    Ack ack = callStub.twoPcCommit(msg);
                    String m = ack.getMsg();
                    if (m == null || m.isBlank()) {
                        delivered = true;
                    }
                    ch.shutdownNow();
                } catch (Exception ignored) {}
            }
            if (delivered) return;
            try { Thread.sleep(300); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
    }

    private void callParticipantAbort(String txnId, int participantCluster) {
        TwoPcAbortMsg msg = TwoPcAbortMsg.newBuilder().setTxnId(txnId).build();
        for (int attempt = 0; attempt < 10; attempt++) {
            String redirect = null;
            boolean delivered = false;
            for (Integer id : st.numericIdsByCluster(participantCluster)) {
                NodeServiceGrpc.NodeServiceBlockingStub stub = rpcStubForNode(id);
                if (stub == null) continue;
                try {
                    NodeServiceGrpc.NodeServiceBlockingStub callStub = maybeCompressStub(stub, msg.getSerializedSize())
                            .withDeadlineAfter(FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    Ack ack = callStub.twoPcAbort(msg);
                    String m = ack.getMsg();
                    if (m != null && m.startsWith("REDIRECT ")) {
                        redirect = m.substring("REDIRECT ".length());
                        break;
                    }
                    if (m == null || m.isBlank()) {
                        return;
                    }

                } catch (Exception ignored) {}
            }
            if (redirect != null && !redirect.isBlank()) {
                try {
                    ManagedChannel ch = NettyChannelBuilder.forTarget(redirect)
                            .usePlaintext()
                            .keepAliveTime(PAXOS_RPC_KEEPALIVE_TIME_SEC, TimeUnit.SECONDS)
                            .keepAliveTimeout(PAXOS_RPC_KEEPALIVE_TIMEOUT_SEC, TimeUnit.SECONDS)
                            .keepAliveWithoutCalls(PAXOS_RPC_KEEPALIVE_WITHOUT_CALLS)
                            .maxInboundMessageSize(PAXOS_RPC_MAX_INBOUND_MESSAGE_MB * 1024 * 1024)
                            .flowControlWindow(PAXOS_RPC_FLOW_CONTROL_WINDOW_BYTES)
                            .withOption(ChannelOption.TCP_NODELAY, PAXOS_RPC_TCP_NODELAY)
                            .build();
                    NodeServiceGrpc.NodeServiceBlockingStub stub2 = NodeServiceGrpc.newBlockingStub(ch);
                    NodeServiceGrpc.NodeServiceBlockingStub callStub = maybeCompressStub(stub2, msg.getSerializedSize())
                            .withDeadlineAfter(FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    Ack ack = callStub.twoPcAbort(msg);
                    String m = ack.getMsg();
                    if (m == null || m.isBlank()) {
                        delivered = true;
                    }
                    ch.shutdownNow();
                } catch (Exception ignored) {}
            }
            if (delivered) return;
            try { Thread.sleep(300); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
    }

    private void discardSpeculativeLocalAccept(long seq) {
        st.rw.writeLock().lock();
        try {
            st.log.remove(seq);
            st.executionInfo.remove(seq);
        } finally {
            st.rw.writeLock().unlock();
        }
    }

    private void notifyExecution(NodeState.ExecutionInfo info) {
        if (info == null) {
            return;
        }
        mExecApplied.incrementAndGet();
        lastProgressMs.set(System.currentTimeMillis());
        CompletableFuture<NodeState.ExecutionInfo> future = executionWaiters.remove(info.seq());
        if (future != null && !future.isDone()) {
            future.complete(info);
        }
    }

    private long currentBacklog() {
        st.rw.readLock().lock();
        try {
            return Math.max(0, st.commitIndex - st.executedIndex);
        } finally {
            st.rw.readLock().unlock();
        }
    }

    private void waitForBacklogBelow(long maxBacklog, long timeoutMs) {
        if (maxBacklog <= 0) return;
        long start = System.currentTimeMillis();
        while (active.get() && currentBacklog() > maxBacklog) {
            if (System.currentTimeMillis() - start >= Math.max(1L, timeoutMs)) break;
            try { Thread.sleep(1); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
        }
    }

    private NodeState.ExecutionInfo waitForExecution(long seq, Duration timeout) {
        Optional<NodeState.ExecutionInfo> existing = st.executionFor(seq);
        if (existing.isPresent()) {
            return existing.get();
        }
        CompletableFuture<NodeState.ExecutionInfo> future = executionWaiters.computeIfAbsent(seq, key -> new CompletableFuture<>());
        try {
            return future.get(Math.max(timeout.toMillis(), 1L), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException ignored) {
        } finally {
            executionWaiters.remove(seq);
        }
        return null;
    }

    private NodeState.ExecutionInfo commitLocal(CommitMsg req) {
        long seq = req.getS().getValue();
        NodeState.ExecutionInfo lastInfo = null;
        boolean shouldContinue = true;

        while (shouldContinue) {
            long gapCandidate = -1L;
            st.rw.writeLock().lock();
            try {
                if (greater(req.getB(), st.highestSeenBallot)) {
                    st.highestSeenBallot = req.getB();
                }
                st.currentLeaderBallot = req.getB();
                st.lastHeartbeatSeenMs = System.currentTimeMillis();

                st.log.compute(seq, (k, existing) -> {
                    NodeState.Phase phase = NodeState.Phase.COMMITTED;
                    if (existing != null && existing.getPhase().ordinal() > phase.ordinal()) {
                        phase = existing.getPhase();
                    }
                    TwoPcStage incoming = req.getStage();
                    TwoPcStage current = (existing != null) ? existing.getTwoPcStage() : TwoPcStage.TWO_PC_NONE;
                    TwoPcStage stg;
                    if (incoming == TwoPcStage.TWO_PC_NONE) {
                        stg = current;
                    } else if (current == TwoPcStage.TWO_PC_COMMIT || current == TwoPcStage.TWO_PC_ABORT) {
                        stg = current;
                    } else {
                        stg = incoming;
                    }
                    return new LogEntry(req.getM(), req.getB(), phase, stg);
                });
                st.markCommitted(seq);

                if (seq > st.commitIndex) {
                    st.commitIndex = seq;
                }
                        if (seq <= st.executedIndex) { }
                if (!PAXOS_APPLIER_THREAD) {
                    while (st.executedIndex + 1 <= st.commitIndex) {
                        long run = st.executedIndex + 1;
                        Optional<Request> nextOpt = st.requestFor(run);
                        if (nextOpt.isEmpty()) {
                            gapCandidate = run;
                            break;
                        }
                        Request next = nextOpt.get();
                        Ballot execBallot = ballotFor(run, req.getB());
                        NodeState.ExecutionInfo info = execute(run, next, execBallot);

                        notifyExecution(info);

                        TwoPcStage stageNow = TwoPcStage.TWO_PC_NONE;
                        LogEntry leNow = st.log.get(run);
                        if (leNow != null) stageNow = leNow.getTwoPcStage();
                        boolean terminal = (stageNow == TwoPcStage.TWO_PC_NONE) ||
                                (stageNow == TwoPcStage.TWO_PC_COMMIT) ||
                                (stageNow == TwoPcStage.TWO_PC_ABORT);
                        if (terminal) {
                            st.executedIndex = run;
                            st.markCommitted(run);
                            lastInfo = info;
                        } else {

                            lastInfo = info;
                            break;
                        }
                    }
                }
            } finally {
                st.rw.writeLock().unlock();
            }

            if (PAXOS_APPLIER_THREAD || gapCandidate == -1L) {
                shouldContinue = false;
            } else {
                boolean fetched = fetchAndApplyCatchup(gapCandidate - 1);
                if (!fetched) {
                    shouldContinue = false;
                } else {
                    try {
                        Thread.sleep(CATCHUP_RETRY_DELAY_MS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        shouldContinue = false;
                    }
                }
            }
        }

        return lastInfo;
    }

    private void applyCommittedLoop() {
        while (active.get()) {
            long gapCandidate = -1L;
            st.rw.writeLock().lock();
            try {
                long next = st.executedIndex + 1;
                if (next > st.commitIndex) {

                } else {
                    Optional<Request> nextOpt = st.requestFor(next);
                    if (nextOpt.isEmpty()) {
                        gapCandidate = next;
                    } else {
                        Request nextReq = nextOpt.get();
                        LogEntry le = st.log.get(next);
                        Ballot base = (le != null && le.getAcceptedBallot() != null) ? le.getAcceptedBallot() : st.currentLeaderBallot;
                        Ballot execBallot = ballotFor(next, base);
                        NodeState.ExecutionInfo info = execute(next, nextReq, execBallot);

                        notifyExecution(info);
                        TwoPcStage stageNow = TwoPcStage.TWO_PC_NONE;
                        LogEntry leNow = st.log.get(next);
                        if (leNow != null) stageNow = leNow.getTwoPcStage();
                        boolean terminal = (stageNow == TwoPcStage.TWO_PC_NONE) ||
                                (stageNow == TwoPcStage.TWO_PC_COMMIT) ||
                                (stageNow == TwoPcStage.TWO_PC_ABORT);
                        if (terminal) {
                            st.executedIndex = next;
                            st.markCommitted(next);
                        }
                    }
                }
            } finally {
                st.rw.writeLock().unlock();
            }
            if (gapCandidate != -1L) {
                boolean fetched = fetchAndApplyCatchup(gapCandidate - 1);
                if (!fetched) {
                    try { Thread.sleep(CATCHUP_RETRY_DELAY_MS); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            } else {
                try { Thread.sleep(1); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }

private boolean fetchAndApplyCatchup_EARLY(long fromExclusive) { return false; }

private void repairLog_EARLY(long fromExclusive) { }

    private boolean fetchAndApplyCatchup(long fromExclusive) {
        int leaderId = st.currentLeaderBallot.getLeaderId();
        int myClusterIdx = (st.nodeNumericId - 1) / 3;
        java.util.List<Integer> inCluster = st.numericIdsByCluster(myClusterIdx);
        java.util.List<Integer> candidates = new java.util.ArrayList<>();
        if (leaderId > 0 && inCluster.contains(leaderId)) {
            candidates.add(leaderId);
        }
        for (Integer id : inCluster) {
            if (id != st.nodeNumericId && id != leaderId) {
                candidates.add(id);
            }
        }

        for (Integer id : candidates) {
            NodeServiceGrpc.NodeServiceBlockingStub stub = stubForNode(id);
            if (stub == null) {
                continue;
            }
            try {
                CatchupRequest request = CatchupRequest.newBuilder()
                        .setB(st.currentLeaderBallot)
                        .setFrom(SeqNo.newBuilder().setValue(Math.max(0, fromExclusive)))
                        .setRequester(NodeId.newBuilder().setId(String.valueOf(st.nodeNumericId)))
                        .build();
                CatchupReply reply = stub.catchup(request);
                if (reply.getCommitsCount() == 0) {
                    if (PAXOS_SNAPSHOT_INSTALL_ENABLE) {
                        try {
                            SnapshotOffer offer = stub.getLocalSnapshot(com.google.protobuf.Empty.getDefaultInstance());
                            if (offer != null && offer.getLastApplied() > st.executedIndex) {
                                applySnapshotOfferLocal(offer);
                                return true;
                            }
                        } catch (Exception ignored) {}
                    }
                    continue;
                }
                st.rw.writeLock().lock();
                try {
                    for (CommitMsg commit : reply.getCommitsList()) {
                        long seq = commit.getS().getValue();
                        st.log.compute(seq, (k, existing) -> {
                            NodeState.Phase phase = NodeState.Phase.COMMITTED;
                            if (existing != null && existing.getPhase().ordinal() > phase.ordinal()) {
                                phase = existing.getPhase();
                            }
                            TwoPcStage incoming = commit.getStage();
                            TwoPcStage current = (existing != null) ? existing.getTwoPcStage() : TwoPcStage.TWO_PC_NONE;
                            TwoPcStage stg;
                            if (incoming == TwoPcStage.TWO_PC_NONE) {
                                stg = current;
                            } else if (current == TwoPcStage.TWO_PC_COMMIT || current == TwoPcStage.TWO_PC_ABORT) {
                                stg = current;
                            } else {
                                stg = incoming;
                            }
                            return new LogEntry(commit.getM(), commit.getB(), phase, stg);
                        });
                        st.markCommitted(seq);
                        if (seq > st.commitIndex) {
                            st.commitIndex = seq;
                        }
                        if (greater(commit.getB(), st.highestSeenBallot)) {
                            st.highestSeenBallot = commit.getB();
                        }
                        if (seq <= st.executedIndex) { }
                    }
                } finally {
                    st.rw.writeLock().unlock();
                }
                return true;
            } catch (Exception ignored) {
            }
        }
        return false;
    }

    private void repairLog(long fromExclusive) {
        int leaderId = st.currentLeaderBallot.getLeaderId();
        int myClusterIdx = (st.nodeNumericId - 1) / 3;
        java.util.List<Integer> inCluster = st.numericIdsByCluster(myClusterIdx);
        java.util.List<Integer> candidates = new java.util.ArrayList<>();
        if (leaderId > 0 && inCluster.contains(leaderId)) {
            candidates.add(leaderId);
        }
        for (Integer id : inCluster) {
            if (id != st.nodeNumericId && id != leaderId) {
                candidates.add(id);
            }
        }

        for (Integer id : candidates) {
            NodeServiceGrpc.NodeServiceBlockingStub stub = stubForNode(id);
            if (stub == null) {
                continue;
            }
            try {
                CatchupRequest request = CatchupRequest.newBuilder()
                        .setB(st.currentLeaderBallot)
                        .setFrom(SeqNo.newBuilder().setValue(Math.max(0, fromExclusive)))
                        .setRequester(NodeId.newBuilder().setId(String.valueOf(st.nodeNumericId)))
                        .build();

                CatchupReply reply = stub.catchup(request);

                if (reply.getCommitsCount() == 0) {
                    continue;
                }

                System.out.printf("[node %d] Repairing log: received %d missing transactions%n",
                        st.nodeNumericId, reply.getCommitsCount());

                st.rw.writeLock().lock();
                try {
                    for (CommitMsg commit : reply.getCommitsList()) {
                        long seq = commit.getS().getValue();
                        st.log.compute(seq, (k, existing) -> {
                            NodeState.Phase phase = NodeState.Phase.COMMITTED;
                            if (existing != null && existing.getPhase().ordinal() > phase.ordinal()) {
                                phase = existing.getPhase();
                            }
                            TwoPcStage stg = commit.getStage();
                            if (stg == TwoPcStage.TWO_PC_NONE && existing != null) {
                                stg = existing.getTwoPcStage();
                            }
                            return new LogEntry(commit.getM(), commit.getB(), phase, stg);
                        });
                        st.markCommitted(seq);
                        if (seq > st.commitIndex) {
                            st.commitIndex = seq;
                        }
                        if (greater(commit.getB(), st.highestSeenBallot)) {
                            st.highestSeenBallot = commit.getB();
                        }
                    }

                    while (st.executedIndex + 1 <= st.commitIndex) {
                        long seq = st.executedIndex + 1;
                        Optional<Request> nextOpt = st.requestFor(seq);
                        if (nextOpt.isEmpty()) {
                            break;
                        }
                        Request next = nextOpt.get();
                        Ballot ballot = ballotFor(seq, st.currentLeaderBallot);
                        NodeState.ExecutionInfo info = execute(seq, next, ballot);

                        notifyExecution(info);

                        TwoPcStage stageNow = TwoPcStage.TWO_PC_NONE;
                        LogEntry leNow = st.log.get(seq);
                        if (leNow != null) stageNow = leNow.getTwoPcStage();
                        boolean terminal = (stageNow == TwoPcStage.TWO_PC_NONE) ||
                                (stageNow == TwoPcStage.TWO_PC_COMMIT) ||
                                (stageNow == TwoPcStage.TWO_PC_ABORT);
                        if (terminal) {
                            st.executedIndex = seq;
                            st.markCommitted(seq);
                        } else {

                            break;
                        }
                    }

                    System.out.printf("[node %d] Repair complete: execIndex now %d%n",
                            st.nodeNumericId, st.executedIndex);
                } finally {
                    st.rw.writeLock().unlock();
                }
                return;
            } catch (Exception ignored) {
            }
        }

        System.out.printf("[node %d] Repair complete: no missing transactions%n", st.nodeNumericId);
    }

    private NodeServiceGrpc.NodeServiceBlockingStub stubForNode(int numericId) {
        String address = st.nodeAddressBook.get(numericId);
        if (address == null || address.isBlank()) {
            return null;
        }
        return catchupStubCache.computeIfAbsent(numericId, id -> {
            ManagedChannel channel = NettyChannelBuilder.forTarget(address)
                    .usePlaintext()
                    .keepAliveTime(PAXOS_RPC_KEEPALIVE_TIME_SEC, TimeUnit.SECONDS)
                    .keepAliveTimeout(PAXOS_RPC_KEEPALIVE_TIMEOUT_SEC, TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(PAXOS_RPC_KEEPALIVE_WITHOUT_CALLS)
                    .maxInboundMessageSize(PAXOS_RPC_MAX_INBOUND_MESSAGE_MB * 1024 * 1024)
                    .flowControlWindow(PAXOS_RPC_FLOW_CONTROL_WINDOW_BYTES)
                    .withOption(ChannelOption.TCP_NODELAY, PAXOS_RPC_TCP_NODELAY)
                    .build();
            catchupChannels.put(id, channel);
            NodeServiceGrpc.NodeServiceBlockingStub stub = NodeServiceGrpc.newBlockingStub(channel);
            if (PAXOS_RPC_ENABLE_GZIP) {
                stub = stub.withCompression("gzip");
            }
            return stub;
        });
    }

    private Ballot ballotFor(long seq, Ballot fallback) {
        return st.ballotFor(seq).orElse(fallback);
    }

    private NodeState.ExecutionInfo execute(long seq, Request req, Ballot ballot) {

        LogEntry preLogEntry = st.log.get(seq);
        TwoPcStage preStage = (preLogEntry != null) ? preLogEntry.getTwoPcStage() : TwoPcStage.TWO_PC_NONE;

        Optional<NodeState.ExecutionInfo> existing = st.executionFor(seq);
        if (existing.isPresent() && preStage == TwoPcStage.TWO_PC_NONE) {

            return existing.get();
        }



        ClientMeta meta = extractMetadata(req);
        if (preStage == TwoPcStage.TWO_PC_NONE && meta != null) {
            Optional<ClientReply> cached = st.cachedReply(meta.getClientId(), meta.getTimestamp());
            if (cached.isPresent()) {
                ClientReply prior = cached.get();
                NodeState.ExecutionInfo info = new NodeState.ExecutionInfo(
                        seq,
                        prior.getSuccess(),
                        prior.getError(),
                        req,
                        ballot);
                st.rememberExecution(info);
                return info;
            }
        }

        boolean success;
        String error;
        if (req.hasTrans()) {
            Transaction t = req.getTrans();
            String sTxt = t.getSender();
            String rTxt = t.getReceiver();
            long amt = t.getAmount();
            Integer sId = parseIntOrNull(sTxt);
            Integer rId = parseIntOrNull(rTxt);

            TwoPcStage stage = preStage;
            String txnId = null;
            ClientMeta m = extractMetadata(req);
            if (m != null) {
                txnId = NodeState.cacheKey(m.getClientId(), m.getTimestamp());
            }

            if (stage != TwoPcStage.TWO_PC_NONE && txnId != null && sId != null && rId != null) {
                try {
                    switch (stage) {
                        case TWO_PC_PREPARE -> {
                            boolean senderLocal = isLocalShardId(sId);
                            boolean receiverLocal = isLocalShardId(rId);

                            if (senderLocal) {
                                st.tryAcquireSingleLock(sTxt);
                            }
                            if (receiverLocal) {
                                st.tryAcquireSingleLock(rTxt);
                            }
                            if (senderLocal) {
                                st.walApplyPrepareDebit(txnId, sId, amt);
                            }
                            if (receiverLocal) {
                                st.walApplyPrepareCredit(txnId, rId, amt);
                            }

                            if (senderLocal && !receiverLocal) {
                                st.twoPc.beginCoordinator(txnId, t, "");
                            } else if (receiverLocal && !senderLocal) {
                                st.twoPc.beginParticipant(txnId, t, "");
                            }
                            st.twoPc.markPreparedLocal(txnId, seq);
                            success = true;
                            error = "";
                        }
                        case TWO_PC_COMMIT -> {

                            st.twoPc.decideCommit(txnId);
                            st.twoPc.markCommitted(txnId, seq);

                            boolean senderLocal = isLocalShardId(sId);
                            boolean receiverLocal = isLocalShardId(rId);
                            if (senderLocal) {
                                st.tryAcquireSingleLock(sTxt);
                                st.walApplyPrepareDebit(txnId, sId, amt);
                            }
                            if (receiverLocal) {
                                st.tryAcquireSingleLock(rTxt);
                                st.walApplyPrepareCredit(txnId, rId, amt);
                            }
                            st.walOnCommit(txnId);
                            st.releaseSingleLock(sTxt);
                            st.releaseSingleLock(rTxt);
                            success = true;
                            error = "";
                        }
                        case TWO_PC_ABORT -> {

                            st.twoPc.decideAbort(txnId);
                            st.twoPc.markAborted(txnId, seq);
                            st.walOnAbort(txnId);
                            st.releaseSingleLock(sTxt);
                            st.releaseSingleLock(rTxt);
                            success = true;
                            error = "";
                        }
                        default -> {
                            BankStateMachine.Result res = st.stateMachine.apply(req);
                            success = res.success();
                            error = res.error();
                        }
                    }
                } catch (IllegalStateException ex) {
                    success = false;
                    error = ex.getMessage();
                }
            } else {
                if (sId != null && rId != null) {
                    boolean senderLocalX = isLocalShardId(sId);
                    boolean receiverLocalX = isLocalShardId(rId);
                    if (senderLocalX && receiverLocalX) {
                        AccountStore.Result res = st.accountStore.transfer(sId, rId, amt);
                        success = res.success();
                        error = res.error();
                        if (success) {
                            st.recordModified(sId);
                            st.recordModified(rId);
                        }
                    } else {



                        success = true;
                        error = "";
                    }
                } else {
                    BankStateMachine.Result res = st.stateMachine.apply(req);
                    success = res.success();
                    error = res.error();
                }
                if (!sTxt.isBlank() || !rTxt.isBlank()) {
                    st.releaseLocks(sTxt, rTxt);
                }
            }
        } else {
            BankStateMachine.Result res = st.stateMachine.apply(req);
            success = res.success();
            error = res.error();
        }

        NodeState.ExecutionInfo info = new NodeState.ExecutionInfo(seq, success, error, req, ballot);
        st.rememberExecution(info);
        return info;
    }

    private ClientMeta extractMetadata(Request req) {
        if (!req.hasClient() || !req.hasTs()) {
            return null;
        }
        String clientId = req.getClient().getId();
        long ts = req.getTs().getT();
        if (clientId.isBlank() || ts <= 0) {
            return null;
        }
        return ClientMeta.newBuilder()
                .setClientId(clientId)
                .setTimestamp(ts)
                .build();
    }

    private ClientReply buildReply(ClientMeta meta, Ballot ballot, boolean success, String error) {
        ClientReply.Builder builder = ClientReply.newBuilder()
                .setMeta(meta)
                .setSuccess(success)
                .setBallot(ballot != null ? ballot : snapshotBallot());
        if (!success && error != null && !error.isBlank()) {
            builder.setError(error);
        } else {
            builder.setError("");
        }
        return builder.build();
    }

    private void stageAccept(AcceptMsg req) {
        boolean noop = NodeState.isNoop(req.getM());
        Request request = noop ? NodeState.NOOP : req.getM();
        AcceptEntry entry = AcceptEntry.newBuilder()
                .setS(req.getS())
                .setSrcBallot(req.getB())
                .setM(request)
                .setIsNoop(noop)
                .setStage(req.getStage())
                .build();
        stageAccept(entry, req.getB());
    }

    private void stageAccept(AcceptEntry entry, Ballot acceptBallot) {
        long seq = entry.getS().getValue();
        Request request = entry.getIsNoop() ? NodeState.NOOP : (entry.hasM() ? entry.getM() : NodeState.NOOP);
        Ballot effectiveBallot = entry.hasSrcBallot() ? entry.getSrcBallot() : acceptBallot;

        Optional<Ballot> existingBallot = st.ballotFor(seq);
        if (existingBallot.isPresent() && Ballots.greater(existingBallot.get(), effectiveBallot)) {
            System.out.printf("[node %d] Ignoring stale accept for seq=%d: existing ballot=%d > new ballot=%d%n",
                    st.nodeNumericId, seq, existingBallot.get().getRound(), effectiveBallot.getRound());
            return;
        }

        st.markAsAccepted(seq, effectiveBallot, request, entry.getStage());
    }

    private void installFromNewView(AcceptEntry entry, Ballot acceptBallot) {
        long seq = entry.getS().getValue();
        Request request = entry.getIsNoop() ? NodeState.NOOP : (entry.hasM() ? entry.getM() : NodeState.NOOP);
        Ballot effectiveBallot = entry.hasSrcBallot() ? entry.getSrcBallot() : acceptBallot;

        st.log.compute(seq, (k, existing) -> {
            NodeState.Phase phase = existing != null ? existing.getPhase() : NodeState.Phase.ACCEPTED;
            TwoPcStage stg = entry.getStage();
            return new LogEntry(request, effectiveBallot, phase, stg);
        });
    }

    private void processNewView(NewViewMsg msg, boolean fromSelf) {
        st.rw.writeLock().lock();
        try {
            if (greater(msg.getB(), st.highestSeenBallot)) {
                st.highestSeenBallot = msg.getB();
            }
            st.currentLeaderBallot = msg.getB();
            st.lastHeartbeatSeenMs = System.currentTimeMillis();
            if (!fromSelf) {
                st.role = Roles.FOLLOWER;
            }

            System.out.printf("[node %d] INSTALLING new view ballot=(round=%d,leader=%d) fromSelf=%s entries=%d%n",
                    st.nodeNumericId,
                    msg.getB().getRound(),
                    msg.getB().getLeaderId(),
                    fromSelf,
                    msg.getEntriesCount());

            long maxSeq = 0L;
            for (AcceptEntry entry : msg.getEntriesList()) {
                installFromNewView(entry, msg.getB());
                long seq = entry.getS().getValue();
                maxSeq = Math.max(maxSeq, seq);
                st.markCommitted(seq);
                if (seq > st.commitIndex) {
                    st.commitIndex = seq;
                }
            }

            while (st.executedIndex < st.commitIndex) {
                long nextExec = st.executedIndex + 1;
                Optional<Request> reqOpt = st.requestFor(nextExec);
                if (reqOpt.isEmpty()) {
                    System.out.printf("[node %d] WARNING: Gap at seq=%d during newView installation%n",
                            st.nodeNumericId, nextExec);
                    break;
                }
                Request req = reqOpt.get();
                Ballot execBallot = ballotFor(nextExec, msg.getB());
                NodeState.ExecutionInfo info = execute(nextExec, req, execBallot);

                notifyExecution(info);

                TwoPcStage stageNow = TwoPcStage.TWO_PC_NONE;
                LogEntry leNow = st.log.get(nextExec);
                if (leNow != null) stageNow = leNow.getTwoPcStage();
                boolean terminal = (stageNow == TwoPcStage.TWO_PC_NONE) ||
                        (stageNow == TwoPcStage.TWO_PC_COMMIT) ||
                        (stageNow == TwoPcStage.TWO_PC_ABORT);
                if (terminal) {
                    st.executedIndex = nextExec;
                } else {
                    break;
                }
                System.out.printf("[node %d] Executed seq=%d during newView installation%n",
                        st.nodeNumericId, nextExec);
            }

            long maxFromLog = st.log.isEmpty() ? 0L : st.log.lastKey();
            long nextSeq = Math.max(maxSeq + 1,
                    Math.max(st.commitIndex + 1,
                            Math.max(st.executedIndex + 1, maxFromLog + 1)));
            st.nextSeq.set(nextSeq);

            System.out.printf("[node %d] newView complete: execIndex=%d, commitIndex=%d, nextSeq=%d%n",
                    st.nodeNumericId, st.executedIndex, st.commitIndex, nextSeq);

            NewViewLog logEntry = NewViewLog.newBuilder()
                    .setTsMs(System.currentTimeMillis())
                    .setB(msg.getB())
                    .setLeader(msg.getFrom())
                    .addAllEntries(msg.getEntriesList())
                    .build();
            st.recordView(logEntry);
            logLatestView(logEntry);
        } finally {
            st.rw.writeLock().unlock();
        }
    }

    private List<NewViewAck> broadcastNewView(NewViewMsg msg) {
        List<NewViewAck> acks = new ArrayList<>();
        if (!active.get()) {
            return acks;
        }

        for (NodeServiceGrpc.NodeServiceBlockingStub stub : peerStubs) {
            try {
                NewViewAck ack = stub.newView(msg);
                acks.add(ack);
                String from = ack.hasFrom() ? ack.getFrom().getId() : "?";
                System.out.printf("[node %d] <- NEW-VIEW ACK from %s (lastApplied=%d)%n",
                        st.nodeNumericId, from, ack.getLastApplied().getValue());
            } catch (Exception ex) {
                System.out.printf("[node %d] peer did not ack NEW-VIEW%n", st.nodeNumericId);
            }
        }
        return acks;
    }

    private boolean broadcastHeartbeat() {
        if (!active.get() || st.role != Roles.LEADER) {
            return false;
        }

        long currentCommitIndex;
        st.rw.readLock().lock();
        try {
            currentCommitIndex = st.commitIndex;
        } finally {
            st.rw.readLock().unlock();
        }

        HeartbeatMsg msg = HeartbeatMsg.newBuilder()
                .setB(st.currentLeaderBallot)
                .setFrom(NodeId.newBuilder().setId(String.valueOf(st.nodeNumericId)))
                .setCommitIndex(currentCommitIndex)
                .build();

        boolean sent = false;
        for (NodeServiceGrpc.NodeServiceBlockingStub stub : peerStubs) {
            if (!active.get() || st.role != Roles.LEADER) {
                break;
            }
            try {
                stub.heartbeat(msg);
                sent = true;
            } catch (Exception ignored) {
            }
        }
        return sent;
    }

    private void logCommitState(String reason) {
        st.rw.readLock().lock();
        try {
            String formattedLog = st.formatLog();
            System.out.printf("[node %d] COMMIT index=%d (%s)%n%s",
                    st.nodeNumericId,
                    st.commitIndex,
                    reason,
                    formattedLog);
        } finally {
            st.rw.readLock().unlock();
        }
    }

    public void deactivate() {
        if (active.compareAndSet(true, false)) {
            failPendingWaiters("inactive");

            int pendingCount = pendingRequests.size();
            for (Map.Entry<String, CompletableFuture<ClientReply>> entry : pendingRequests.entrySet()) {
                if (!entry.getValue().isDone()) {
                    String key = entry.getKey();
                    String[] parts = key.split(":", 2);
                    if (parts.length == 2) {
                        try {
                            ClientMeta meta = ClientMeta.newBuilder()
                                    .setClientId(parts[0])
                                    .setTimestamp(Long.parseLong(parts[1]))
                                    .build();
                            entry.getValue().complete(failureReply(meta, "NODE_INACTIVE"));
                        } catch (NumberFormatException ex) {
                            entry.getValue().completeExceptionally(
                                    new IllegalStateException("Node deactivated"));
                        }
                    } else {
                        entry.getValue().completeExceptionally(
                                new IllegalStateException("Node deactivated"));
                    }
                }
            }
            pendingRequests.clear();

            DeferredPrepare pending = deferredPrepare.getAndSet(null);
            if (pending != null) {
                pending.resp.onError(Status.UNAVAILABLE.withDescription("NODE_INACTIVE").asRuntimeException());
            }
            cancelDeferredPrepareTask();


            System.out.printf("[node %d] Node deactivated (cleared %d pending, preserving state: role=%s)%n",
                    st.nodeNumericId, pendingCount, st.role);
        }
    }

    public void activate() {
        if (active.compareAndSet(false, true)) {
            long now = System.currentTimeMillis();
            boolean wasLeader = (st.role == Roles.LEADER);

            if (wasLeader) {

                st.lastHeartbeatSeenMs = now;
                System.out.printf("[node %d] Node reactivated as LEADER - will send heartbeat immediately%n",
                        st.nodeNumericId);


                scheduler.execute(() -> {
                    if (broadcastHeartbeat()) {
                        lastHeartbeatBroadcastMs = System.currentTimeMillis();
                    }
                });
            } else {

                long timeSinceLastHeartbeat = now - st.lastHeartbeatSeenMs;
                System.out.printf("[node %d] Node reactivated as %s - preserving old timers (last heartbeat was %d ms ago)%n",
                        st.nodeNumericId,
                        st.role,
                        timeSinceLastHeartbeat);


                scheduler.schedule(this::proactiveCatchup, 300, TimeUnit.MILLISECONDS);
            }


            scheduler.execute(this::rebuildTwoPcOnStartup);

            scheduler.schedule(this::resumeTwoPcAsLeader, 200, TimeUnit.MILLISECONDS);
        }
    }

    private void scheduleLeaderFailureDeactivation(long maxDelayMs) {
        final long start = System.currentTimeMillis();
        System.out.printf("[node %d] LEADER FAILURE - Scheduling deactivation with grace up to %d ms (pending=%d)%n",
                st.nodeNumericId, maxDelayMs, pendingRequests.size());

        Runnable poll = new Runnable() {
            @Override
            public void run() {
                if (!active.get()) {
                    return;
                }
                long elapsed = System.currentTimeMillis() - start;
                int pending = pendingRequests.size();
                if (pending == 0 || elapsed >= maxDelayMs) {
                    System.out.printf("[node %d] LEADER FAILURE - Grace period over (pending=%d, elapsed=%dms). Deactivating now.%n",
                            st.nodeNumericId, pending, elapsed);
                    deactivateForLeaderFailure();
                } else {
                    scheduler.schedule(this, 50, TimeUnit.MILLISECONDS);
                }
            }
        };
        scheduler.schedule(poll, 50, TimeUnit.MILLISECONDS);
    }

    private void deactivateForLeaderFailure() {
        if (active.compareAndSet(true, false)) {
            failPendingWaiters("leader-failure");

            for (Map.Entry<String, CompletableFuture<ClientReply>> entry : pendingRequests.entrySet()) {
                if (!entry.getValue().isDone()) {
                    String key = entry.getKey();
                    String[] parts = key.split(":", 2);
                    if (parts.length == 2) {
                        try {
                            ClientMeta meta = ClientMeta.newBuilder()
                                    .setClientId(parts[0])
                                    .setTimestamp(Long.parseLong(parts[1]))
                                    .build();
                            entry.getValue().complete(failureReply(meta, "LEADER_FAILURE"));
                        } catch (NumberFormatException ex) {
                            entry.getValue().completeExceptionally(
                                    new IllegalStateException("Leader failure"));
                        }
                    } else {
                        entry.getValue().completeExceptionally(
                                new IllegalStateException("Leader failure"));
                    }
                }
            }
            pendingRequests.clear();

            DeferredPrepare pending = deferredPrepare.getAndSet(null);
            if (pending != null) {
                pending.resp.onError(Status.UNAVAILABLE.withDescription("LEADER_FAILURE").asRuntimeException());
            }
            cancelDeferredPrepareTask();

            st.rw.writeLock().lock();
            try {
                if (st.role == Roles.LEADER) {
                    st.role = Roles.FOLLOWER;
                }

            } finally {
                st.rw.writeLock().unlock();
            }

            System.out.printf("[node %d] LEADER FAILURE - Deactivated and demoted to follower (state destroyed)%n",
                    st.nodeNumericId);
        }
    }

    private void proactiveCatchup() {
        if (!active.get()) {
            System.out.printf("[node %d] Skipping proactive catchup - node inactive%n",
                    st.nodeNumericId);
            return;
        }

        long currentExecIndex;
        long currentCommitIndex;
        int leaderId;

        st.rw.readLock().lock();
        try {
            currentExecIndex = st.executedIndex;
            currentCommitIndex = st.commitIndex;
            leaderId = st.currentLeaderBallot.getLeaderId();
        } finally {
            st.rw.readLock().unlock();
        }

        System.out.printf("[node %d] Proactive catchup check: execIndex=%d, commitIndex=%d, leaderId=%d%n",
                st.nodeNumericId, currentExecIndex, currentCommitIndex, leaderId);

        if (leaderId == st.nodeNumericId) {
            System.out.printf("[node %d] Skipping proactive catchup - I am the leader%n",
                    st.nodeNumericId);
            return;
        }

        if (leaderId <= 0) {
            repairLog(currentExecIndex);
            scheduler.schedule(this::proactiveCatchup, 500, TimeUnit.MILLISECONDS);
            return;
        }

        System.out.printf("[node %d] Initiating proactive catchup from leader %d (from index %d)%n",
                st.nodeNumericId, leaderId, currentExecIndex);

        repairLog(currentExecIndex);
    }




    private void rebuildTwoPcOnStartup() {
        if (!active.get()) return;
        try {

            for (NodeState.WalRecord rec : st.walValues()) {
                String txnId = rec.txnId;
                if (rec.senderId > 0) {
                    String sKey = String.valueOf(rec.senderId);
                    st.tryAcquireSingleLock(sKey);
                    Transaction tx = Transaction.newBuilder()
                            .setSender(sKey)
                            .setReceiver("")
                            .setAmount(rec.amount)
                            .build();
                    st.twoPc.beginCoordinator(txnId, tx, "");
                    st.twoPc.markPreparedLocal(txnId, 0L);
                }
                if (rec.receiverId > 0) {
                    String rKey = String.valueOf(rec.receiverId);
                    st.tryAcquireSingleLock(rKey);
                    Transaction tx = Transaction.newBuilder()
                            .setSender("")
                            .setReceiver(rKey)
                            .setAmount(rec.amount)
                            .build();
                    st.twoPc.beginParticipant(txnId, tx, "");
                    st.twoPc.markPreparedLocal(txnId, 0L);
                }
            }


            st.log.forEach((seq, entry) -> {
                if (seq > st.commitIndex) return;
                TwoPcStage stage = entry.getTwoPcStage();
                if (stage == TwoPcStage.TWO_PC_NONE) return;
                Request r = entry.getRequest();
                if (!r.hasClient() || !r.hasTs() || !r.hasTrans()) return;
                String txnId = NodeState.cacheKey(r.getClient().getId(), r.getTs().getT());
                Transaction t = r.getTrans();
                Integer sId = parseIntOrNull(t.getSender());
                Integer rId = parseIntOrNull(t.getReceiver());
                boolean senderLocal = sId != null && isLocalShardId(sId);
                boolean receiverLocal = rId != null && isLocalShardId(rId);
                if (senderLocal && !receiverLocal) {
                    st.twoPc.beginCoordinator(txnId, t, "");
                } else if (receiverLocal && !senderLocal) {
                    st.twoPc.beginParticipant(txnId, t, "");
                }
                switch (stage) {
                    case TWO_PC_PREPARE -> st.twoPc.markPreparedLocal(txnId, seq);
                    case TWO_PC_COMMIT -> { st.twoPc.decideCommit(txnId); st.twoPc.markCommitted(txnId, seq); }
                    case TWO_PC_ABORT -> { st.twoPc.decideAbort(txnId); st.twoPc.markAborted(txnId, seq); }
                    default -> {}
                }
            });


            long from = st.executedIndex;
            long to = st.commitIndex;
            while (from < to) {
                long next = from + 1;
                Optional<Request> reqOpt = st.requestFor(next);
                if (reqOpt.isEmpty()) break;
                Ballot execBallot = ballotFor(next, snapshotBallot());
                NodeState.ExecutionInfo info = execute(next, reqOpt.get(), execBallot);
                notifyExecution(info);
                TwoPcStage stageNow = TwoPcStage.TWO_PC_NONE;
                LogEntry leNow = st.log.get(next);
                if (leNow != null) stageNow = leNow.getTwoPcStage();
                boolean terminal = (stageNow == TwoPcStage.TWO_PC_NONE) ||
                        (stageNow == TwoPcStage.TWO_PC_COMMIT) ||
                        (stageNow == TwoPcStage.TWO_PC_ABORT);
                if (terminal) {
                    st.executedIndex = next;
                    from = next;
                } else {
                    break;
                }
            }

            resumeTwoPcAsLeader();
        } catch (Exception ignored) {
        }
    }


    private void resumeTwoPcAsLeader() {
        if (!active.get() || st.role != Roles.LEADER) return;

        java.util.List<Long> seqs = new java.util.ArrayList<>();
        java.util.List<LogEntry> entries = new java.util.ArrayList<>();
        st.rw.readLock().lock();
        try {
            st.log.forEach((seq, entry) -> {
                if (seq <= st.commitIndex) {
                    entries.add(entry);
                    seqs.add(seq);
                }
            });
        } finally {
            st.rw.readLock().unlock();
        }

        for (int i = 0; i < entries.size(); i++) {
            LogEntry e = entries.get(i);
            TwoPcStage stage = e.getTwoPcStage();
            if (stage == TwoPcStage.TWO_PC_NONE) continue;
            Request r = e.getRequest();
            if (!r.hasClient() || !r.hasTs() || !r.hasTrans()) continue;
            Transaction t = r.getTrans();
            Integer sId = parseIntOrNull(t.getSender());
            Integer rId = parseIntOrNull(t.getReceiver());
            if (sId == null || rId == null) continue;
            boolean senderLocal = isLocalShardId(sId.intValue());
            boolean receiverLocal = isLocalShardId(rId.intValue());
            if (!(senderLocal && !receiverLocal)) continue;
            String txnId = NodeState.cacheKey(r.getClient().getId(), r.getTs().getT());
            int participantCluster = st.clusterIndexForAccountId(rId.intValue());

            try {
                switch (stage) {
                    case TWO_PC_PREPARE -> {
                        Request paxosReq = Request.newBuilder()
                                .setClient(r.getClient())
                                .setTs(r.getTs())
                                .setTrans(t)
                                .build();

                        TwoPcManager.TxnState ts = st.twoPc.get(txnId).orElse(null);
                        boolean remotePreparedBefore = (ts != null && ts.preparedRemote);
                        if (remotePreparedBefore) {
                            OptionalLong commitSeq = replicateAsLeaderWithStage(paxosReq, TwoPcStage.TWO_PC_COMMIT);
                            commitSeq.ifPresent(s -> waitForExecution(s, EXECUTION_WAIT));
                            callParticipantCommit(txnId, participantCluster);
                            break;
                        }
                        boolean preparedRemote = callParticipantPrepare(txnId, t, participantCluster);
                        if (preparedRemote) {
                            st.twoPc.markPreparedRemote(txnId);
                            OptionalLong commitSeq = replicateAsLeaderWithStage(paxosReq, TwoPcStage.TWO_PC_COMMIT);
                            commitSeq.ifPresent(s -> waitForExecution(s, EXECUTION_WAIT));
                            callParticipantCommit(txnId, participantCluster);
                        } else {
                            OptionalLong abortSeq = replicateAsLeaderWithStage(paxosReq, TwoPcStage.TWO_PC_ABORT);
                            abortSeq.ifPresent(s -> waitForExecution(s, EXECUTION_WAIT));
                            callParticipantAbort(txnId, participantCluster);
                        }
                    }
                    case TWO_PC_COMMIT -> {

                        callParticipantCommit(txnId, participantCluster);
                    }
                    case TWO_PC_ABORT -> {

                        callParticipantAbort(txnId, participantCluster);
                    }
                    default -> {}
                }
            } catch (IllegalStateException ignored) {

                return;
            }
        }
    }

    private static String pretty(Request r) {
        return r.toString().replaceAll("\\s+", " ");
    }

    private ClientReply handleSubmitAsLeader(ClientRequest req, ClientMeta meta) {
        Transaction tx = req.hasTx() ? req.getTx() : Transaction.getDefaultInstance();
        if (tx.getSender().isEmpty() || tx.getReceiver().isEmpty()) {
            return failureReply(meta, "INVALID_TX");
        }

        String s = tx.getSender();
        String r = tx.getReceiver();
        long amt = tx.getAmount();
        Integer sId = parseIntOrNull(s);
        Integer rId = parseIntOrNull(r);
        boolean hasIds = (sId != null && rId != null);
        boolean senderLocal = (sId != null) && isLocalShardId(sId.intValue());
        boolean receiverLocal = (rId != null) && isLocalShardId(rId.intValue());


        if (hasIds && senderLocal && !receiverLocal) {

            if (!tryAcquireSingleLockWithTimer(s)) {
                return failureReply(meta, "LOCKED");
            }


            long currentBal = st.accountStore.getBalance(sId);
            if (currentBal < amt) {
                st.releaseSingleLock(s);
                return failureReply(meta, "INSUFFICIENT_FUNDS");
            }

            String txnId = NodeState.cacheKey(meta.getClientId(), meta.getTimestamp());
            st.twoPc.beginCoordinator(txnId, tx, "");

            Request paxosRequest = Request.newBuilder()
                    .setClient(ClientId.newBuilder().setId(meta.getClientId()))
                    .setTs(Timestamp.newBuilder().setT(meta.getTimestamp()))
                    .setTrans(tx)
                    .build();


            int participantCluster = st.clusterIndexForAccountId(rId.intValue());
            boolean preparedRemote = callParticipantPrepare(txnId, tx, participantCluster);
            if (!preparedRemote) {

                OptionalLong abortSeq = replicateAsLeaderWithStage(paxosRequest, TwoPcStage.TWO_PC_ABORT);
                abortSeq.ifPresent(seq -> waitForExecution(seq, EXECUTION_WAIT));
                if (abortSeq.isPresent()) {
                    callParticipantAbort(txnId, participantCluster);
                }
                st.rememberReply(failureReply(meta, "ABORTED"));
                st.releaseSingleLock(s);
                return failureReply(meta, "ABORTED");
            }


            OptionalLong prepSeq;
            try {
                prepSeq = replicateAsLeaderWithStage(paxosRequest, TwoPcStage.TWO_PC_PREPARE);
            } catch (IllegalStateException ex) {
                OptionalLong abortSeq = replicateAsLeaderWithStage(paxosRequest, TwoPcStage.TWO_PC_ABORT);
                abortSeq.ifPresent(seq -> waitForExecution(seq, EXECUTION_WAIT));
                if (abortSeq.isPresent()) {
                    callParticipantAbort(txnId, participantCluster);
                }
                st.releaseSingleLock(s);
                return failureReply(meta, "NOT_LEADER");
            }
            if (prepSeq.isEmpty()) {
                OptionalLong abortSeq = replicateAsLeaderWithStage(paxosRequest, TwoPcStage.TWO_PC_ABORT);
                abortSeq.ifPresent(seq -> waitForExecution(seq, EXECUTION_WAIT));
                if (abortSeq.isPresent()) {
                    callParticipantAbort(txnId, participantCluster);
                }
                st.releaseSingleLock(s);
                return failureReply(meta, "REPLICATION_FAILED");
            }
            NodeState.ExecutionInfo prepInfo = waitForExecution(prepSeq.getAsLong(), EXECUTION_WAIT);
            if (prepInfo == null || !prepInfo.success()) {
                OptionalLong abortSeq = replicateAsLeaderWithStage(paxosRequest, TwoPcStage.TWO_PC_ABORT);
                abortSeq.ifPresent(seq -> waitForExecution(seq, EXECUTION_WAIT));
                if (abortSeq.isPresent()) {
                    callParticipantAbort(txnId, participantCluster);
                }
                st.releaseSingleLock(s);
                return failureReply(meta, prepInfo == null ? "EXECUTION_TIMEOUT" : (prepInfo.error() == null ? "EXECUTION_FAILED" : prepInfo.error()));
            }
            st.twoPc.markPreparedLocal(txnId, prepSeq.getAsLong());


            st.twoPc.markPreparedRemote(txnId);
            st.twoPc.decideCommit(txnId);
            OptionalLong commitSeq = replicateAsLeaderWithStage(paxosRequest, TwoPcStage.TWO_PC_COMMIT);
            commitSeq.ifPresent(seq -> waitForExecution(seq, EXECUTION_WAIT));
            commitSeq.ifPresent(seq -> st.twoPc.markCommitted(txnId, seq));
            callParticipantCommit(txnId, participantCluster);

            ClientReply reply = buildReply(meta, snapshotBallot(), true, "");
            st.rememberReply(reply);
            return reply;
        }


        if (hasIds && (!senderLocal || !receiverLocal)) {
            return failureReply(meta, "CROSS_SHARD");
        }
        boolean acquired = st.tryAcquireLocks(s, r);
        if (!acquired) {
            return failureReply(meta, "LOCKED");
        }

        Request paxosRequest = Request.newBuilder()
                .setClient(ClientId.newBuilder().setId(meta.getClientId()))
                .setTs(Timestamp.newBuilder().setT(meta.getTimestamp()))
                .setTrans(tx)
                .build();

        OptionalLong seqOpt;
        try {
            seqOpt = replicateAsLeaderWithStage(paxosRequest, TwoPcStage.TWO_PC_NONE);
        } catch (IllegalStateException ex) {
            st.releaseLocks(s, r);
            return failureReply(meta, "NOT_LEADER");
        }

        if (seqOpt.isEmpty()) {
            st.releaseLocks(s, r);
            return failureReply(meta, "REPLICATION_FAILED");
        }

        NodeState.ExecutionInfo info = waitForExecution(seqOpt.getAsLong(), EXECUTION_WAIT);
        if (info == null) {
            st.releaseLocks(s, r);
            return failureReply(meta, "EXECUTION_TIMEOUT");
        }

        ClientReply reply = st.cachedReply(meta.getClientId(), meta.getTimestamp())
                .orElseGet(() -> buildReply(meta, info.ballot(), info.success(), info.error()))
                .toBuilder()
                .setBallot(isSafe(info.ballot()))
                .build();

        st.rememberReply(reply);
        System.out.printf("[node %d] SUBMIT client=%s ts=%d sender=%s receiver=%s amount=%d -> success=%s error=%s%n",
                st.nodeNumericId,
                meta.getClientId(),
                meta.getTimestamp(),
                tx.getSender(),
                tx.getReceiver(),
                tx.getAmount(),
                reply.getSuccess(),
                reply.getError());
        return reply;
    }

    private Ballot isSafe(Ballot ballot) {
        return ballot != null ? ballot : snapshotBallot();
    }

    private ClientReply failureReply(ClientMeta meta, String error) {
        return ClientReply.newBuilder()
                .setMeta(meta)
                .setSuccess(false)
                .setError(error)
                .setBallot(snapshotBallot())
                .build();
    }

    private boolean isLocalShardId(int id) {
        int myClusterIdx = (st.nodeNumericId - 1) / 3;
        return st.clusterIndexForAccountId(id) == myClusterIdx;
    }

    private Ballot snapshotBallot() {
        st.rw.readLock().lock();
        try {
            return st.role == Roles.LEADER ? st.currentLeaderBallot : st.highestSeenBallot;
        } finally {
            st.rw.readLock().unlock();
        }
    }

    private void logLatestView(NewViewLog logEntry) {
        String leaderId = logEntry.hasLeader() ? logEntry.getLeader().getId() : "?";
        long round = logEntry.getB().getRound();
        StringBuilder entriesSummary = new StringBuilder();
        for (AcceptEntry entry : logEntry.getEntriesList()) {
            long seq = entry.getS().getValue();
            boolean noop = entry.getIsNoop();
            entriesSummary.append(String.format(" #%d%s", seq, noop ? "(noop)" : ""));
        }
        System.out.printf("[node %d] NEWVIEW leaderLog: leader=%s round=%d entries=%d%s%n",
                st.nodeNumericId,
                leaderId,
                round,
                logEntry.getEntriesCount(),
                entriesSummary);
    }

    private Optional<ClientReply> forwardRequesttoLeader(ClientRequest req, ClientMeta meta) {
        int leaderId = st.currentLeaderBallot.getLeaderId();
        if (leaderId <= 0 || leaderId == st.nodeNumericId) {
            leaderId = st.highestSeenBallot.getLeaderId();
        }
        if (leaderId <= 0 || leaderId == st.nodeNumericId) {
            return Optional.empty();
        }

        NodeServiceGrpc.NodeServiceBlockingStub stub = stubForNode(leaderId);
        if (stub == null) {
            return Optional.empty();
        }

        try {
            ClientReply reply = stub.withDeadlineAfter(FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS).submit(req);
            return Optional.of(reply);
        } catch (Exception ex) {
            return Optional.empty();
        }
    }

    private ClientReply redirectToLeader(ClientMeta meta) {
        int leaderId = st.currentLeaderBallot.getLeaderId();
        if (leaderId == 0) {
            leaderId = st.highestSeenBallot.getLeaderId();
        }
        if (leaderId == st.nodeNumericId && st.role != Roles.LEADER) {
            leaderId = 0;
        }

        String leaderAddress = st.addressForNode(leaderId).orElse("");
        if (!leaderAddress.isEmpty()) {
            System.out.printf("[node %d] redirecting client=%s ts=%d to leader %s%n",
                    st.nodeNumericId, meta.getClientId(), meta.getTimestamp(), leaderAddress);
        } else {
            System.out.printf("[node %d] cannot determine leader for client=%s ts=%d%n",
                    st.nodeNumericId, meta.getClientId(), meta.getTimestamp());
        }

        return ClientReply.newBuilder()
                .setMeta(meta)
                .setSuccess(false)
                .setError(leaderAddress.isEmpty() ? "NO_LEADER" : leaderAddress)
                .setBallot(snapshotBallot())
                .build();
    }

    //used chatGPT to write this function
    public Map<Integer, NodeState.Phase> collectClusterStatus(long seq) {
        Map<Integer, NodeState.Phase> statuses = new LinkedHashMap<>();
        statuses.put(st.nodeNumericId, st.phaseFor(seq));

        st.nodeAddressBook.forEach((id, address) -> {
            if (id == st.nodeNumericId) {
                return;
            }
            NodeServiceGrpc.NodeServiceBlockingStub stub = stubForNode(id);
            if (stub == null) {
                return;
            }
            try {
                StatusRequest request = StatusRequest.newBuilder()
                        .setS(SeqNo.newBuilder().setValue(seq))
                        .build();
                StatusDump dump = stub.withDeadlineAfter(FORWARD_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                        .printStatus(request);
                if (dump.getEntriesCount() > 0) {
                    StatusEntry entry = dump.getEntries(0);
                    String phaseText = entry.getPhase();
                    NodeState.Phase phase = decodePhase(phaseText);
                    statuses.put(id, phase);
                }
            } catch (Exception ignored) {
            }
        });
        return statuses;
    }

    private NodeState.Phase decodePhase(String encoded) {
        if (encoded == null || encoded.isEmpty()) {
            return NodeState.Phase.NONE;
        }
        char symbol = encoded.charAt(0);
        for (NodeState.Phase phase : NodeState.Phase.values()) {
            if (phase.symbol() == symbol) {
                return phase;
            }
        }
        return NodeState.Phase.NONE;
    }

    private boolean ensureActive(StreamObserver<?> resp) {
        if (active.get()) {
            return true;
        }
        resp.onError(Status.UNAVAILABLE.withDescription("NODE_INACTIVE").asRuntimeException());
        return false;
    }

    private void failPendingWaiters(String reason) {
        IllegalStateException ex = new IllegalStateException(reason);
        for (Map.Entry<Long, CompletableFuture<NodeState.ExecutionInfo>> entry : executionWaiters.entrySet()) {
            entry.getValue().completeExceptionally(ex);
        }
        executionWaiters.clear();
    }

    private static final class DeferredPrepare {
        final PrepareMsg req;
        final StreamObserver<PromiseMsg> resp;
        final AtomicBoolean completed = new AtomicBoolean(false);

        DeferredPrepare(PrepareMsg req, StreamObserver<PromiseMsg> resp) {
            this.req = req;
            this.resp = resp;
        }

        boolean markCompleted() {
            return completed.compareAndSet(false, true);
        }
    }
}