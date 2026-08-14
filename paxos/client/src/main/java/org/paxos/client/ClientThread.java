package org.paxos.client;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.paxos.proto.Ballot;
import org.paxos.proto.ClientMeta;
import org.paxos.proto.ClientReply;
import org.paxos.proto.ClientRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;



public final class ClientThread implements Runnable, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ClientThread.class);
    private static final double BACKOFF_FACTOR = 2.0d;
    private static final long BACKOFF_CAP_MULTIPLIER = 4L;
    private static final long LEADER_TIMEOUT_MS = 500L;
    private static final int MAX_ATTEMPTS = 20;
    private static final WorkItem POISON = WorkItem.poison();
    private static final boolean CLIENT_STICKY_LEADER = Boolean.parseBoolean(
            System.getenv().getOrDefault("CLIENT_STICKY_LEADER", "true"));

    private final String clientId;
    private final ClientConfig config;
    private final NodeDirectory nodeDirectory;
    private final BlockingQueue<WorkItem> queue = new LinkedBlockingQueue<>();
    private final PerformanceRecorder perf;
    private final AtomicLong lastTimestamp = new AtomicLong(0L);
    private final AtomicReference<LeaderHint> leaderHint = new AtomicReference<>();
    private final AtomicReference<CountDownLatch> setGate = new AtomicReference<>(new CountDownLatch(0));

    private volatile boolean running = true;
    private Thread thread;

    public ClientThread(String clientId, ClientConfig config, NodeDirectory nodeDirectory, PerformanceRecorder perf) {
        this.clientId = Objects.requireNonNull(clientId, "clientId");
        this.config = Objects.requireNonNull(config, "config");
        this.nodeDirectory = Objects.requireNonNull(nodeDirectory, "nodeDirectory");
        this.perf = Objects.requireNonNull(perf, "perf");
    }

    public void start() {
        if (thread != null) {
            throw new IllegalStateException("Client thread already started for " + clientId);
        }
        thread = new Thread(this, "client-" + clientId);
        thread.start();
    }

    public void submitWork(int setNumber,
                           List<CsvSetParser.CsvTransaction> transactions,
                           Set<String> liveNodes,
                           CountDownLatch doneSignal) {
        Objects.requireNonNull(transactions, "transactions");
        Objects.requireNonNull(liveNodes, "liveNodes");
        Objects.requireNonNull(doneSignal, "doneSignal");
        if (transactions.isEmpty()) {
            doneSignal.countDown();
            return;
        }
        queue.add(new WorkItem(setNumber, List.copyOf(transactions), List.copyOf(liveNodes), doneSignal, false));
    }

    

    @Override
    public void run() {
        while (running) {
            WorkItem item;
            try {
                item = queue.take();
            } catch (InterruptedException e) {
                if (!running) {
                    break;
                }
                Thread.currentThread().interrupt();
                continue;
            }

            if (item.poison) {
                CountDownLatch latch = item.completion;
                if (latch != null) {
                    latch.countDown();
                }
                break;
            }

            awaitSetGate();

            try {
                handleWork(item);
            } catch (RuntimeException ex) {
                LOG.error("Client {} encountered error while processing set {}", clientId, item.setNumber, ex);
            } finally {
                if (item.completion != null) {
                    item.completion.countDown();
                }
            }
        }
    }

    private void handleWork(WorkItem item) {
        for (CsvSetParser.CsvTransaction tx : item.transactions) {
            processTransaction(item.setNumber, tx, item.liveNodes);
        }
    }

    private void processTransaction(int setNumber,
                                    CsvSetParser.CsvTransaction tx,
                                    List<String> liveNodes) {

        long timestamp = nextTimestamp();
        processTransactionInternal(setNumber, tx, liveNodes, timestamp);
    }

    private void processTransactionInternal(int setNumber,
                                            CsvSetParser.CsvTransaction tx,
                                            List<String> liveNodes,
                                            long timestamp) {

        ClientRequest request = Messages.buildRequest(clientId, timestamp, tx);
        final long startNs = System.nanoTime();

        Optional<NodeDirectory.NodeEndpoint> forcedTarget = config.forcedTargetOpt()
                .flatMap(nodeDirectory::endpointFor);
        if (config.forcedTargetOpt().isPresent() && forcedTarget.isEmpty()) {


        }

        LeaderHint hintSnapshot = leaderHint.get();
        List<String> clusterLive = restrictToSenderCluster(tx, liveNodes);
        List<String> targetsIds;
        if (clusterLive.isEmpty()) {
            Integer sid = parseIntOrNull(tx.sender());
            List<String> clusterAliases = (sid == null) ? List.of() : nodeDirectory.clusterAliasesForId(sid);
            targetsIds = clusterAliases.isEmpty() ? liveNodes : clusterAliases;
        } else {
            targetsIds = clusterLive;
        }

        Optional<NodeDirectory.NodeEndpoint> preferred = forcedTarget;
        if (preferred.isEmpty()) {
            preferred = choosePreferredEndpoint(targetsIds, hintSnapshot);
        }

        List<NodeDirectory.NodeEndpoint> broadcastTargets = endpointsForBroadcast(targetsIds);
        if (broadcastTargets.isEmpty()) {


            return;
        }

        long baseTimeout = Math.max(config.timeoutMs(), LEADER_TIMEOUT_MS);
        long timeoutForAttempt = baseTimeout;

        final AtomicBoolean successFlag = new AtomicBoolean(false);
        String lastError = null;
        boolean broadcastMode = false;
        final boolean forcedTargetMode = forcedTarget.isPresent();
        int attempt = 0;
        int consecutiveTimeouts = 0;

        while (running && !successFlag.get() && attempt < MAX_ATTEMPTS) {
            attempt++;
            boolean redirectedThisAttempt = false;
            boolean receivedResponseThisAttempt = false;

            List<NodeDirectory.NodeEndpoint> targets;
            if (!broadcastMode || forcedTargetMode) {
                if (preferred.isPresent()) {
                    targets = List.of(preferred.get());
                } else if (!broadcastTargets.isEmpty()) {
                    targets = List.of(broadcastTargets.get(0));
                } else {
                    targets = List.of();
                }
            } else {
                targets = broadcastTargets;
            }

            for (NodeDirectory.NodeEndpoint endpoint : targets) {
                if (!running || successFlag.get()) {
                    break;
                }

                String targetLabel = endpoint.describe();
                try {
                    ClientReply reply = endpoint.submit(request, timeoutForAttempt);
                    receivedResponseThisAttempt = true;
                    consecutiveTimeouts = 0;

                    ClientMeta replyMeta = reply.hasMeta() ? reply.getMeta() : ClientMeta.getDefaultInstance();

                    // Validate response metadata
                    if (!clientId.equals(replyMeta.getClientId())) {
//                        LOG.warn("[client {}] set={} tx=({}, {}, {}) ts={} attempt={} -> target={} result=ERROR reason=client-mismatch expected={} got={}",
//                                clientId, setNumber, tx.sender(), tx.receiver(), tx.amount(), timestamp, attempt, targetLabel, clientId, replyMeta.getClientId());
                        continue;
                    }
                    if (replyMeta.getTimestamp() != timestamp) {
//                        LOG.warn("[client {}] set={} tx=({}, {}, {}) ts={} attempt={} -> target={} result=ERROR reason=timestamp-mismatch got={}",
//                                clientId, setNumber, tx.sender(), tx.receiver(), tx.amount(), timestamp, attempt, targetLabel, replyMeta.getTimestamp());
                        continue;
                    }

                    Ballot ballot = reply.hasBallot() ? reply.getBallot() : null;
                    updateLeaderHint(endpoint, ballot, targetsIds);
                    Optional<NodeDirectory.NodeEndpoint> ballotLeader = endpointForLeader(ballot, targetsIds);

                    if (reply.getSuccess()) {
                        final long endNs = System.nanoTime();
                        perf.record(setNumber, startNs, endNs);

                        if (tx != null) {
                            HistoryCollector.getDefault().record(tx.sender(), tx.receiver(), tx.amount(), timestamp);
                        }
                        successFlag.set(true);
                        Ballot safeBallot = ballot != null ? ballot : Ballot.getDefaultInstance();
//                        LOG.info("[client {}] set={} tx=({}, {}, {}) ts={} attempt={} -> target={} result=OK ballot=(round={},leader={})",
//                                clientId, setNumber, tx.sender(), tx.receiver(), tx.amount(), timestamp, attempt, targetLabel,
//                                safeBallot.getRound(), safeBallot.getLeaderId());
                        if (forcedTarget.isEmpty() && ballotLeader.isPresent()) {
                            preferred = ballotLeader;
                        }
                        break;
                    }

                    String error = reply.getError();
                    lastError = error;

                    if ("STALE_TIMESTAMP".equals(error)) {
//                        LOG.info("[client {}] set={} tx=({}, {}, {}) ts={} attempt={} already processed (stale)",
//                                clientId, setNumber, tx.sender(), tx.receiver(), tx.amount(), timestamp, attempt);
                        final long endNs = System.nanoTime();
                        perf.record(setNumber, startNs, endNs);
                        successFlag.set(true);
                        break;
                    }

                    if ("INSUFFICIENT_FUNDS".equals(error)) {
                        final long endNs = System.nanoTime();
                        perf.record(setNumber, startNs, endNs);
                        break;
                    }
                    if ("INVALID_TX".equals(error)) {
                        final long endNs = System.nanoTime();
                        perf.record(setNumber, startNs, endNs);
                        break;
                    }
                    if ("LOCKED".equals(error)) {

                        final long endNs = System.nanoTime();
                        perf.record(setNumber, startNs, endNs);
                        break;
                    }


                    boolean retargetedFromBallot = false;
                    if (forcedTarget.isEmpty() && ballotLeader.isPresent()) {
                        NodeDirectory.NodeEndpoint newLeader = ballotLeader.get();
                        if (!newLeader.target().equals(endpoint.target())) {
                            preferred = ballotLeader;
                            broadcastMode = false;
                            redirectedThisAttempt = true;
                            retargetedFromBallot = true;
//                            LOG.info("[client {}] set={} ts={} attempt={} learned leader from ballot -> new-target={}",
//                                    clientId, setNumber, timestamp, attempt, newLeader.describe());
                            break;
                        }
                    }

                    if (!retargetedFromBallot && forcedTarget.isEmpty() && error != null && !error.isBlank()) {
                        String redirect = error.trim();
                        if (redirect.matches("[^\\s:]+:[0-9]{1,5}")) {
                            try {
                                Optional<NodeDirectory.NodeEndpoint> redirected = nodeDirectory.endpointFor(redirect);
                                if (redirected.isPresent()) {
                                    NodeDirectory.NodeEndpoint redirectEndpoint = redirected.get();
                                    if (!targetsIds.contains(redirectEndpoint.nodeId())) {

                                    } else {
                                        preferred = redirected;
                                        broadcastMode = false;
                                        redirectedThisAttempt = true;
                                        break;
                                    }
                                }
                            } catch (IllegalArgumentException ex) {
                                LOG.debug("[client {}] invalid redirect target '{}'", clientId, redirect, ex);
                            }
                        }
                    }

//                    LOG.warn("[client {}] set={} tx=({}, {}, {}) ts={} attempt={} -> target={} result=ERROR reason={}",
//                            clientId, setNumber, tx.sender(), tx.receiver(), tx.amount(), timestamp, attempt, targetLabel,
//                            (error == null || error.isBlank()) ? "server-rejected" : error);

                } catch (StatusRuntimeException ex) {
                    consecutiveTimeouts++;
                    Status status = ex.getStatus();
                    String reason = status.getCode() == Status.Code.DEADLINE_EXCEEDED
                            ? String.format("timeout after %d ms", timeoutForAttempt)
                            : status.toString();
//                    LOG.warn("[client {}] set={} tx=({}, {}, {}) ts={} attempt={} -> target={} result=ERROR reason={}",
//                            clientId, setNumber, tx.sender(), tx.receiver(), tx.amount(), timestamp, attempt, targetLabel, reason);
                    if (!forcedTargetMode) {
                        preferred = Optional.empty();
                    }
                }
            }

            if (successFlag.get()) {
                break;
            }

            if (redirectedThisAttempt) {
                timeoutForAttempt = baseTimeout;
                continue;
            }

            if (!broadcastMode && !forcedTargetMode && receivedResponseThisAttempt) {
                broadcastMode = true;
                timeoutForAttempt = baseTimeout;

//                LOG.info("[client {}] set={} tx=({}, {}, {}) ts={} attempt={} -> escalating to broadcast across {} targets",
//                        clientId, setNumber, tx.sender(), tx.receiver(), tx.amount(), timestamp, attempt, broadcastTargets.size());
                continue;
            }

            if (consecutiveTimeouts >= 2) {
                timeoutForAttempt = nextBackoff(timeoutForAttempt, baseTimeout);
                long sleepMs = Math.min(timeoutForAttempt / 2, 500);
                sleepQuietly(Duration.ofMillis(sleepMs));
            }
        }


    }



    private long nextTimestamp() {
        return lastTimestamp.incrementAndGet();
    }

    private Optional<NodeDirectory.NodeEndpoint> choosePreferredEndpoint(List<String> liveNodes, LeaderHint hint) {
        if (hint != null && liveNodes.contains(hint.nodeId())) {
            return nodeDirectory.endpointFor(hint.nodeId());
        }
        if (CLIENT_STICKY_LEADER) {
            String leaderAlias = nodeDirectory.getCurrentLeaderNodeId();
            if (leaderAlias != null && liveNodes.contains(leaderAlias)) {
                Optional<NodeDirectory.NodeEndpoint> ep = nodeDirectory.endpointFor(leaderAlias);
                if (ep.isPresent()) return ep;
            }
        }
        List<NodeDirectory.NodeEndpoint> first = firstAvailableEndpoint(liveNodes);
        return first.isEmpty() ? Optional.empty() : Optional.of(first.get(0));
    }

    private static Integer parseIntOrNull(String s) {
        if (s == null) return null;
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private List<String> restrictToSenderCluster(CsvSetParser.CsvTransaction tx, List<String> liveNodes) {
        Integer sid = parseIntOrNull(tx.sender());
        if (sid == null) {
            return List.of();
        }
        List<String> aliases = nodeDirectory.clusterAliasesForId(sid);
        if (aliases.isEmpty()) {
            return List.of();
        }
        List<String> filtered = new ArrayList<>(3);
        for (String a : aliases) {
            if (liveNodes.contains(a)) {
                filtered.add(a);
            }
        }
        return filtered;
    }

    private List<NodeDirectory.NodeEndpoint> firstAvailableEndpoint(List<String> liveNodes) {
        for (String nodeId : liveNodes) {
            Optional<NodeDirectory.NodeEndpoint> endpoint = nodeDirectory.endpointFor(nodeId);
            if (endpoint.isPresent()) {
                return List.of(endpoint.get());
            }
        }
        return List.of();
    }

    private List<NodeDirectory.NodeEndpoint> endpointsForBroadcast(List<String> liveNodes) {
        List<NodeDirectory.NodeEndpoint> endpoints = new ArrayList<>(liveNodes.size());
        for (String nodeId : liveNodes) {
            nodeDirectory.endpointFor(nodeId).ifPresent(endpoints::add);
        }
        return endpoints;
    }

    private Optional<NodeDirectory.NodeEndpoint> endpointForLeader(Ballot ballot, List<String> liveNodes) {
        if (ballot == null || ballot.getLeaderId() <= 0) {
            return Optional.empty();
        }
        Optional<NodeDirectory.NodeEndpoint> endpoint = nodeDirectory.endpointForNumericId(ballot.getLeaderId());
        if (endpoint.isEmpty()) {
            return Optional.empty();
        }
        if (!liveNodes.contains(endpoint.get().nodeId())) {
            return Optional.empty();
        }
        return endpoint;
    }

    private void updateLeaderHint(NodeDirectory.NodeEndpoint responder, Ballot ballot, List<String> liveNodes) {
        if (ballot == null || responder == null) {
            return;
        }
        NodeDirectory.NodeEndpoint chosen = endpointForLeader(ballot, liveNodes).orElse(responder);
        leaderHint.updateAndGet(previous -> {
            Ballot previousBallot = previous == null ? null : previous.ballot();
            if (previous == null || isHigher(ballot, previousBallot)) {
                return new LeaderHint(chosen.nodeId(), ballot);
            }
            if (previousBallot == null) {
                return previous;
            }
            if (!chosen.nodeId().equals(previous.nodeId()) && previousBallot.getLeaderId() == ballot.getLeaderId()) {
                return new LeaderHint(chosen.nodeId(), previousBallot);
            }
            return previous;
        });
        if (chosen != null) {
            chosen.numericId().ifPresent(nodeDirectory::recordLeader);
        } else if (ballot.getLeaderId() > 0) {
            nodeDirectory.recordLeader(ballot.getLeaderId());
        }
    }

    private static boolean isHigher(Ballot candidate, Ballot current) {
        if (candidate == null) {
            return false;
        }
        long roundDiff = candidate.getRound() - (current != null ? current.getRound() : 0L);
        if (roundDiff != 0) {
            return roundDiff > 0;
        }
        int leaderDiff = candidate.getLeaderId() - (current != null ? current.getLeaderId() : 0);
        return leaderDiff > 0;
    }

    private static long nextBackoff(long currentTimeout, long baseTimeout) {
        long capped = baseTimeout * BACKOFF_CAP_MULTIPLIER;
        double next = Math.ceil(currentTimeout * BACKOFF_FACTOR);
        return Math.min((long) next, capped);
    }

    private static void sleepQuietly(Duration duration) {
        try {
            TimeUnit.MILLISECONDS.sleep(Math.max(duration.toMillis(), 1L));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private void awaitSetGate() {
        try {
            setGate.get().await();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    public void pauseForUserInput() {
        setGate.set(new CountDownLatch(1));
    }

    public void resumeAfterUserInput() {
        CountDownLatch latch = setGate.get();
        if (latch.getCount() > 0) {
            latch.countDown();
        }
    }

    public void shutdown() {
        running = false;
        resumeAfterUserInput();
        queue.offer(POISON);
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(TimeUnit.SECONDS.toMillis(1));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    private record LeaderHint(String nodeId, Ballot ballot) {
    }

    public void resetForNewSet() {
        lastTimestamp.set(0L);
        leaderHint.set(null);
    }

    private static final class WorkItem {
        private final int setNumber;
        private final List<CsvSetParser.CsvTransaction> transactions;
        private final List<String> liveNodes;
        private final CountDownLatch completion;
        private final boolean poison;

        private WorkItem(int setNumber,
                         List<CsvSetParser.CsvTransaction> transactions,
                         List<String> liveNodes,
                         CountDownLatch completion,
                         boolean poison) {
            this.setNumber = setNumber;
            this.transactions = transactions;
            this.liveNodes = liveNodes;
            this.completion = completion;
            this.poison = poison;
        }

        private static WorkItem poison() {
            return new WorkItem(-1, Collections.emptyList(), Collections.emptyList(), null, true);
        }
    }
}
