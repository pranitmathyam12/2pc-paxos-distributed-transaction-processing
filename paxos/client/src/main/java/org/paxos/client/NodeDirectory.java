package org.paxos.client;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import org.paxos.proto.ClientReply;
import org.paxos.proto.ClientRequest;
import org.paxos.proto.NewViewLog;
import org.paxos.proto.NodeControl;

import org.paxos.proto.NodeServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.Empty;
import org.paxos.proto.AcceptEntry;
import org.paxos.proto.BalanceReply;
import org.paxos.proto.BalanceRequest;
import org.paxos.proto.DBDump;
import org.paxos.proto.LogDump;
import org.paxos.proto.PrintViewDump;
import org.paxos.proto.Request;
import org.paxos.proto.SeqNo;
import org.paxos.proto.StatusDump;
import org.paxos.proto.StatusEntry;
import org.paxos.proto.StatusRequest;
import org.paxos.proto.Transaction;

public final class NodeDirectory implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NodeDirectory.class);
    private static final boolean PAXOS_PERF_MODE = Boolean.parseBoolean(
            System.getenv().getOrDefault("PAXOS_PERF_MODE", "false"));

    private static final boolean CLIENT_TCP_NODELAY = Boolean.parseBoolean(
            System.getenv().getOrDefault("CLIENT_TCP_NODELAY", "true"));
    private static final long CLIENT_KEEPALIVE_TIME_SEC = Long.parseLong(
            System.getenv().getOrDefault("CLIENT_KEEPALIVE_TIME_SEC", "30"));
    private static final long CLIENT_KEEPALIVE_TIMEOUT_SEC = Long.parseLong(
            System.getenv().getOrDefault("CLIENT_KEEPALIVE_TIMEOUT_SEC", "10"));
    private static final boolean CLIENT_KEEPALIVE_WITHOUT_CALLS = Boolean.parseBoolean(
            System.getenv().getOrDefault("CLIENT_KEEPALIVE_WITHOUT_CALLS", "true"));
    private static final int CLIENT_MAX_INBOUND_MESSAGE_MB = Integer.parseInt(
            System.getenv().getOrDefault("CLIENT_MAX_INBOUND_MESSAGE_MB", "8"));
    private static final boolean CLIENT_ENABLE_GZIP = Boolean.parseBoolean(
            System.getenv().getOrDefault("CLIENT_ENABLE_GZIP", "false"));
    private static final int CLIENT_FLOW_CONTROL_WINDOW_BYTES = Integer.parseInt(
            System.getenv().getOrDefault("CLIENT_FLOW_CONTROL_WINDOW_BYTES", PAXOS_PERF_MODE ? "2097152" : "1048576"));
    private static final String CLIENT_COMPRESSION_MODE = System.getenv().getOrDefault("CLIENT_COMPRESSION_MODE", PAXOS_PERF_MODE ? "auto" : "off");
    private static final int CLIENT_COMPRESSION_THRESHOLD_BYTES = Integer.parseInt(
            System.getenv().getOrDefault("CLIENT_COMPRESSION_THRESHOLD_BYTES", "65536"));

    private final Map<String, NodeEndpoint> endpointsByAlias;
    private final Map<String, NodeEndpoint> endpointsByTarget;
    private final ConcurrentMap<Integer, NodeEndpoint> endpointsByNumericId;
    private final AtomicInteger lastKnownLeader = new AtomicInteger(0);
    private final ConcurrentMap<Integer, Integer> clusterLeader = new ConcurrentHashMap<>();
    private volatile ShardMapper shardMapper;

    public NodeDirectory(Map<String, String> nodeTargets) {
        Objects.requireNonNull(nodeTargets, "nodeTargets");
        Map<String, NodeEndpoint> aliasMap = new LinkedHashMap<>();
        ConcurrentMap<String, NodeEndpoint> targetMap = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, NodeEndpoint> numericMap = new ConcurrentHashMap<>();
        nodeTargets.forEach((nodeId, target) -> {
            NodeEndpoint endpoint = newEndpoint(nodeId, target);
            aliasMap.put(nodeId, endpoint);
            targetMap.put(target, endpoint);
            endpoint.numericId().ifPresent(id -> numericMap.putIfAbsent(id, endpoint));
        });
        this.endpointsByAlias = Map.copyOf(aliasMap);
        this.endpointsByTarget = targetMap;
        this.endpointsByNumericId = numericMap;
        this.shardMapper = ShardMapper.loadOrDefault();
    }

    private static NodeServiceGrpc.NodeServiceBlockingStub maybeCompressClientStub(NodeServiceGrpc.NodeServiceBlockingStub base, int serializedBytes) {
        String mode = CLIENT_COMPRESSION_MODE == null ? "off" : CLIENT_COMPRESSION_MODE.trim().toLowerCase();
        switch (mode) {
            case "always":
                return base.withCompression("gzip");
            case "auto":
                return (serializedBytes >= Math.max(0, CLIENT_COMPRESSION_THRESHOLD_BYTES)) ? base.withCompression("gzip") : base;
            default:
                return base;
        }
    }

    public void resetAllNodes() {
        for (NodeEndpoint ep : endpointsByAlias.values()) {
            try {
                ep.stub.withDeadlineAfter(2000, TimeUnit.MILLISECONDS).reset(Empty.getDefaultInstance());
            } catch (Exception ignored) {
            }
        }
    }

    public void printBalanceForId(int id) {
        List<String> aliases = clusterAliasesForId(id);
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        for (String nodeId : aliases) {
            NodeEndpoint ep = endpointsByAlias.get(nodeId);
            if (!first) sb.append(", ");
            if (ep == null) {
                sb.append(nodeId).append(" : DOWN");
                first = false;
                continue;
            }
            try {
                BalanceReply r = ep.getBalance(id, 1000);
                sb.append(nodeId).append(" : ").append(r.getBalance());
            } catch (Exception ex) {
                sb.append(nodeId).append(" : N/A");
            }
            first = false;
        }
        System.out.println(sb.toString());
    }

    public List<String> clusterAliasesForId(int id) {
        int clusterIdx = shardMapper.clusterIndexForAccountId(id);
        java.util.List<String> aliases = new java.util.ArrayList<>();
        for (Map.Entry<String, NodeEndpoint> e : endpointsByAlias.entrySet()) {
            int num = e.getValue().numericId().orElse(-1);
            if (num <= 0) continue;
            int idx = (num - 1) / 3;
            if (idx == clusterIdx) aliases.add(e.getKey());
        }
        aliases.sort((a, b) -> {
            int na = parseNumericId(a);
            int nb = parseNumericId(b);
            return Integer.compare(na, nb);
        });
        return aliases;
    }

    public String leaderAliasForId(int id) {
        List<String> aliases = clusterAliasesForId(id);
        return aliases.isEmpty() ? null : aliases.get(0);
    }

    public void resetClientSideState() {
        lastKnownLeader.set(0);
        clusterLeader.clear();
        this.shardMapper = ShardMapper.loadOrDefault();
    }

    public List<String> nodeIds() {
        return List.copyOf(endpointsByAlias.keySet());
    }

    public Optional<NodeEndpoint> endpointFor(String aliasOrTarget) {
        if (aliasOrTarget == null) {
            return Optional.empty();
        }
        NodeEndpoint aliasEndpoint = endpointsByAlias.get(aliasOrTarget);
        if (aliasEndpoint != null) {
            return Optional.of(aliasEndpoint);
        }
        NodeEndpoint existing = endpointsByTarget.get(aliasOrTarget);
        if (existing != null) {
            return Optional.of(existing);
        }
        NodeEndpoint created = newEndpoint(aliasOrTarget, aliasOrTarget);
        NodeEndpoint previous = endpointsByTarget.putIfAbsent(aliasOrTarget, created);
        if (previous != null) {
            created.shutdown();
            return Optional.of(previous);
        }
        created.numericId().ifPresent(id -> endpointsByNumericId.putIfAbsent(id, created));
        return Optional.of(created);
    }

    public Optional<NodeEndpoint> endpointForNumericId(int numericId) {
        if (numericId <= 0) {
            return Optional.empty();
        }
        return Optional.ofNullable(endpointsByNumericId.get(numericId));
    }

    public void applyNodeActivity(Set<String> activeAliases) {
        Objects.requireNonNull(activeAliases, "activeAliases");
        endpointsByAlias.forEach((alias, endpoint) -> {
            boolean shouldBeActive = activeAliases.contains(alias);
            try {
                endpoint.setActive(shouldBeActive);
            } catch (Exception ex) {
                LOG.warn("Failed to set node {} active={} : {}", alias, shouldBeActive, ex.getMessage());
            }
        });


        for (int attempt = 0; attempt < 3; attempt++) {
            boolean stable = true;
            for (Map.Entry<String, NodeEndpoint> e : endpointsByAlias.entrySet()) {
                String alias = e.getKey();
                NodeEndpoint ep = e.getValue();
                boolean shouldBeActive = activeAliases.contains(alias);
                boolean isReadyActive = false;
                try {

                    int nid = ep.numericId().orElseGet(() -> {
                        String digits = alias.replaceAll("\\D", "");
                        try { return Integer.parseInt(digits); } catch (Exception ex) { return -1; }
                    });
                    int clusterIdx = (nid > 0) ? ((nid - 1) / 3) : 0;
                    int sentinel = (clusterIdx == 0) ? 1001 : (clusterIdx == 1 ? 3001 : 6001);

                    long bal = ep.getBalance(sentinel, 400).getBalance();

                    isReadyActive = (bal > 0);
                } catch (Exception ignored) {
                    isReadyActive = false;
                }
                if (isReadyActive != shouldBeActive) {
                    try {
                        ep.setActive(shouldBeActive);
                    } catch (Exception ex) {
                        LOG.debug("Retry setActive failed for {} -> {}: {}", alias, shouldBeActive, ex.toString());
                    }
                    stable = false;
                }
            }
            if (stable) break;
            try {
                Thread.sleep(150);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public void freezeAll() {
        Set<NodeEndpoint> unique = ConcurrentHashMap.newKeySet();
        unique.addAll(endpointsByAlias.values());
        unique.addAll(endpointsByTarget.values());
        unique.addAll(endpointsByNumericId.values());
        for (NodeEndpoint endpoint : unique) {
            try {
                endpoint.setActive(false);
            } catch (Exception ex) {
                LOG.warn("Failed to freeze node {} ({}): {}", endpoint.nodeId(), endpoint.target(), ex.getMessage());
            }
        }
        lastKnownLeader.set(0);
    }

    public void recordLeader(int numericId) {
        if (numericId > 0) {
            lastKnownLeader.set(numericId);
            int idx = (numericId - 1) / 3;
            clusterLeader.put(idx, numericId);
        }
    }

    public String getCurrentLeaderNodeId() {
        int leaderId = lastKnownLeader.get();
        if (leaderId <= 0) {
            return null;
        }
        Optional<NodeEndpoint> endpoint = endpointForNumericId(leaderId);
        return endpoint.map(NodeEndpoint::nodeId).orElse(null);
    }

    public Optional<NodeEndpoint> leaderEndpointForAccount(int id, Set<String> liveAliases) {
        int clusterIdx = shardMapper.clusterIndexForAccountId(id);
        Integer leaderNum = clusterLeader.get(clusterIdx);
        if (leaderNum == null) {
            return Optional.empty();
        }
        Optional<NodeEndpoint> ep = endpointForNumericId(leaderNum);
        if (ep.isEmpty()) {
            return Optional.empty();
        }
        if (liveAliases != null) {
            if (!liveAliases.contains(ep.get().nodeId())) {
                return Optional.empty();
            }
            java.util.List<String> clusterAliases = clusterAliasesForId(id);
            int liveCount = 0;
            for (String alias : clusterAliases) {
                if (liveAliases.contains(alias)) liveCount++;
            }
            int clusterSize = Math.max(1, clusterAliases.size());
            int majority = (clusterSize / 2) + 1;
            if (liveCount < majority) {
                return Optional.empty();
            }
        }
        return ep;
    }

    public void deactivateLeader() {
        int leaderId = lastKnownLeader.get();
        if (leaderId <= 0) {
            LOG.warn("Cannot deactivate leader: no leader known");
            return;
        }

        Optional<NodeEndpoint> endpointOpt = endpointForNumericId(leaderId);
        if (endpointOpt.isEmpty()) {
            LOG.warn("Cannot deactivate leader {}: no endpoint found", leaderId);
            return;
        }

        NodeEndpoint endpoint = endpointOpt.get();
        try {

            NodeControl request = NodeControl.newBuilder()
                    .setActive(false)
                    .setIsLeaderFailure(true)
                    .build();
            endpoint.stub.withDeadlineAfter(500, TimeUnit.MILLISECONDS).control(request);
//            LOG.info("Deactivated leader {} ({}) for leader failure", endpoint.nodeId(), endpoint.target());
        } catch (Exception ex) {
            LOG.warn("Failed to deactivate leader {} ({}): {}",
                    endpoint.nodeId(), endpoint.target(), ex.getMessage());
        }
    }

    // I have used chatGPT to generate this functions. I am aware of the functionality of these functions

    public void printLogAllNodes() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("CLUSTER LOG DUMP - All Transaction Logs");
        System.out.println("=".repeat(80));
        for (int cluster = 0; cluster < 3; cluster++) {
            String[] clusterNodes = cluster == 0 ? new String[]{"n1","n2","n3"}
                    : cluster == 1 ? new String[]{"n4","n5","n6"}
                    : new String[]{"n7","n8","n9"};
            System.out.printf("\n[Cluster %d] %s\n", cluster + 1, String.join(", ", clusterNodes));
            for (String nodeId : clusterNodes) {
                NodeEndpoint endpoint = endpointsByAlias.get(nodeId);
                if (endpoint == null) {
                    System.out.printf("\n--- Node %s ---\n(unconfigured in --nodes)\n", nodeId);
                    continue;
                }
                try {
                    LogDump dump = endpoint.printLog(1000);
                    System.out.printf("\n--- Node %s (ID: %d) ---%n", nodeId, dump.getNodeId());
                    String content = dump.getContent();
                    if (content == null || content.isBlank()) {
                        System.out.println("<no entries>");
                    } else {
                        java.util.List<java.util.List<String>> rows = parseLogRows(content);
                        if (rows.isEmpty()) {
                            System.out.println(content);
                        } else {
                            printTable(null, java.util.List.of("Seq","Phase","Round","2PC","Client","TS","Sender","Receiver","Amt","Result"), rows);
                        }
                    }
                } catch (Exception ex) {
                    System.out.printf("\n--- Node %s: ERROR ---%n", nodeId);
                    System.out.printf("Unable to retrieve log: %s\n", ex.getMessage());
                }
            }
        }
        System.out.println("=".repeat(80) + "\n");
    }

    public void printDBAllNodes() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("CLUSTER DATABASE DUMP - Modified Items per Node");
        System.out.println("(Only data items modified during the current test set are shown)\n");
        System.out.println("=".repeat(80));

        for (int cluster = 0; cluster < 3; cluster++) {
            String[] clusterNodes = cluster == 0 ? new String[]{"n1","n2","n3"}
                    : cluster == 1 ? new String[]{"n4","n5","n6"}
                    : new String[]{"n7","n8","n9"};
            System.out.printf("\n[Cluster %d] %s\n", cluster + 1, String.join(", ", clusterNodes));
            for (String nodeId : clusterNodes) {
                NodeEndpoint endpoint = endpointsByAlias.get(nodeId);
                if (endpoint == null) {
                    System.out.printf("\n--- Node %s ---\n(unconfigured in --nodes)\n", nodeId);
                    continue;
                }
                try {
                    DBDump dump = endpoint.printDB(1000);
                    System.out.printf("\n--- Node %s (ID: %d) ---%n", nodeId, dump.getNodeId());
                    String content = dump.getContent();
                    if (content == null || content.isBlank()) {
                        System.out.println("<none>");
                    } else {
                        String[] lines = content.split("\\R");
                        java.util.List<java.util.List<String>> rows = new java.util.ArrayList<>();
                        for (String line : lines) {
                            String trimmed = line.trim();
                            if (trimmed.isEmpty() || trimmed.startsWith("--")) continue;
                            String[] parts = trimmed.split("\\s+");
                            if (parts.length >= 2) {
                                rows.add(java.util.List.of(parts[0], parts[1]));
                            }
                        }
                        printTable(null, java.util.List.of("ID", "Balance"), rows);
                    }
                } catch (Exception ex) {
                    System.out.printf("\n--- Node %s: ERROR ---%n", nodeId);
                    System.out.printf("Unable to retrieve database: %s\n", ex.getMessage());
                }
            }
        }
        System.out.println("=".repeat(80) + "\n");
    }

    public void printStatusAllNodes(long seqNum) {
        System.out.println("\n" + "=".repeat(80));
        System.out.printf("CLUSTER STATUS - Sequence Number: %d\n", seqNum);
        System.out.println("=".repeat(80));
        System.out.printf("%-15s %-10s %-20s\n", "Node", "Phase", "Description");
        System.out.println("-".repeat(80));

        for (Map.Entry<String, NodeEndpoint> entry : endpointsByAlias.entrySet()) {
            String nodeId = entry.getKey();
            NodeEndpoint endpoint = entry.getValue();
            try {
                StatusDump dump = endpoint.printStatus(seqNum, 1000);
                if (dump.getEntriesCount() > 0) {
                    StatusEntry statusEntry = dump.getEntries(0);
                    String phase = normalizePhaseSymbol(statusEntry.getPhase());
                    String description = getPhaseDescription(phase);
                    System.out.printf("%-15s %-10s %-20s\n", nodeId, phase, description);
                } else {
                    System.out.printf("%-15s %-10s %-20s\n", nodeId, "X", "No Status");
                }
            } catch (Exception ex) {
                System.out.printf("%-15s %-10s %-20s\n", nodeId, "ERROR", ex.getMessage());
            }
        }
        System.out.println("=".repeat(80) + "\n");
    }

    public void printViewAllNodes() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("NEW-VIEW MESSAGES (Cluster-wide)");
        System.out.println("=".repeat(80));


        java.util.List<NewViewLog> all = new java.util.ArrayList<>();
        for (NodeEndpoint ep : endpointsByAlias.values()) {
            try {
                PrintViewDump dump = ep.printView(3000);
                all.addAll(dump.getItemsList());
            } catch (Exception ignored) {}
        }

        if (all.isEmpty()) {
            System.out.println("(No new-view messages recorded in this test set)\n");
            System.out.println("=".repeat(80) + "\n");
            return;
        }


        Map<String, NewViewLog> unique = new LinkedHashMap<>();
        for (NewViewLog v : all) {
            long round = v.getB().getRound();
            String leader = v.hasLeader() ? v.getLeader().getId() : String.valueOf(v.getB().getLeaderId());
            String key = round + ":" + leader;

            NewViewLog prev = unique.get(key);
            if (prev == null || v.getTsMs() < prev.getTsMs()) {
                unique.put(key, v);
            }
        }

        java.util.List<NewViewLog> ordered = new java.util.ArrayList<>(unique.values());
        ordered.sort(java.util.Comparator.comparingLong(NewViewLog::getTsMs));

        java.util.List<java.util.List<String>> rows = new java.util.ArrayList<>();
        int idx = 1;
        for (NewViewLog view : ordered) {
            String leaderStr = view.hasLeader() ? view.getLeader().getId() : String.valueOf(view.getB().getLeaderId());
            String timestamp = new java.text.SimpleDateFormat("HH:mm:ss.SSS").format(new java.util.Date(view.getTsMs()));

            long minSeq = Long.MAX_VALUE, maxSeq = Long.MIN_VALUE;
            int noops = 0;
            for (AcceptEntry e : view.getEntriesList()) {
                long s = e.getS().getValue();
                minSeq = Math.min(minSeq, s);
                maxSeq = Math.max(maxSeq, s);
                if (e.getIsNoop()) noops++;
            }
            String span = (minSeq == Long.MAX_VALUE) ? "-" : (minSeq == maxSeq ? String.valueOf(minSeq) : (minSeq + ".." + maxSeq));
            rows.add(java.util.List.of(String.valueOf(idx++), String.valueOf(view.getB().getRound()), leaderStr, timestamp, String.valueOf(view.getEntriesCount()), span, String.valueOf(noops)));
        }
        printTable("New-View Summary", java.util.List.of("#","Round","Leader","Time","Entries","SeqSpan","Noops"), rows);


        System.out.println("\nDetails per node:");
        for (Map.Entry<String, NodeEndpoint> e : endpointsByAlias.entrySet()) {
            final String nodeId = e.getKey();
            final NodeEndpoint ep = e.getValue();
            System.out.printf("\n--- Node %s ---%n", nodeId);
            try {
                PrintViewDump dump = ep.printView(3000);
                if (dump.getItemsCount() == 0) {
                    System.out.println("  (No view changes recorded)");
                    continue;
                }
                for (int i = 0; i < dump.getItemsCount(); i++) {
                    NewViewLog view = dump.getItems(i);
                    String leaderStr = view.hasLeader() ? view.getLeader().getId() : String.valueOf(view.getB().getLeaderId());
                    String timestamp = new java.text.SimpleDateFormat("HH:mm:ss.SSS").format(new java.util.Date(view.getTsMs()));
                    System.out.printf("  View #%d [%s] round=%d leader=%s entries=%d\n", i + 1, timestamp, view.getB().getRound(), leaderStr, view.getEntriesCount());
                }
            } catch (Exception ex) {
                System.out.printf("  ERROR: Unable to retrieve view history - %s\n", ex.getMessage());
            }
        }

        System.out.println("\n" + "=".repeat(80) + "\n");
    }

    private static java.util.List<java.util.List<String>> parseLogRows(String content) {
        String[] lines = content.split("\\R");
        java.util.List<java.util.List<String>> rows = new java.util.ArrayList<>();
        for (String line : lines) {
            String s = line.trim();
            if (!s.startsWith("#")) continue;

            int sp = s.indexOf(' ');
            String seq = sp > 0 ? s.substring(1, sp) : "-";

            int lb = s.indexOf('[');
            int rb = s.indexOf(']', lb + 1);
            String phase = (lb >= 0 && rb > lb) ? s.substring(lb + 1, rb) : "-";

            String round = "-";
            int bi = s.indexOf("b=", rb + 1);
            if (bi >= 0) {
                int be = s.indexOf(' ', bi + 2);
                round = be > 0 ? s.substring(bi + 2, be) : s.substring(bi + 2);
            }
            String stage = "-";
            int si = s.indexOf("stage=", rb + 1);
            if (si >= 0) {
                int ss = si + 6;
                int se = ss;
                while (se < s.length() && s.charAt(se) != ' ') se++;
                stage = s.substring(ss, se);
            }

            String client = extractQuotedAfter(s, "id:");

            String ts = extractNumberAfter(s, "ts { t:");

            String sender = extractQuotedAfter(s, "sender:");
            String recv = extractQuotedAfter(s, "receiver:");
            String amt = extractNumberAfter(s, "amount:");

            String result = "-";
            int arrow = s.indexOf("->");
            if (arrow >= 0) {
                result = s.substring(arrow + 2).trim();
            }
            rows.add(java.util.List.of(z(seq), z(phase), z(round), z(stage), z(client), z(ts), z(sender), z(recv), z(amt), z(result)));
        }
        return rows;
    }

    private static String extractQuotedAfter(String s, String needle) {
        int i = s.indexOf(needle);
        if (i < 0) return "-";
        int q1 = s.indexOf('"', i);
        if (q1 < 0) return "-";
        int q2 = s.indexOf('"', q1 + 1);
        if (q2 < 0) return "-";
        return s.substring(q1 + 1, q2);
    }

    private static String extractNumberAfter(String s, String needle) {
        int i = s.indexOf(needle);
        if (i < 0) return "-";
        int start = i + needle.length();
        while (start < s.length() && s.charAt(start) == ' ') start++;
        int end = start;
        while (end < s.length() && Character.isDigit(s.charAt(end))) end++;
        if (end == start) return "-";
        return s.substring(start, end);
    }

    private static String z(String v) { return (v == null || v.isBlank()) ? "-" : v; }

    private static void printTable(String title, java.util.List<String> headers, java.util.List<java.util.List<String>> rows) {
        int cols = headers.size();
        int[] widths = new int[cols];
        for (int i = 0; i < cols; i++) widths[i] = headers.get(i).length();
        for (java.util.List<String> r : rows) {
            for (int i = 0; i < cols && i < r.size(); i++) widths[i] = Math.max(widths[i], r.get(i) == null ? 0 : r.get(i).length());
        }
        StringBuilder sep = new StringBuilder();
        for (int w : widths) sep.append("+").append("-".repeat(w + 2));
        sep.append("+");
        if (title != null && !title.isBlank()) System.out.println(title);
        System.out.println(sep);
        StringBuilder headerLine = new StringBuilder();
        for (int i = 0; i < cols; i++) {
            headerLine.append("| ").append(pad(headers.get(i), widths[i])).append(" ");
        }
        headerLine.append("|");
        System.out.println(headerLine);
        System.out.println(sep);
        for (java.util.List<String> r : rows) {
            StringBuilder line = new StringBuilder();
            for (int i = 0; i < cols; i++) {
                String cell = i < r.size() ? (r.get(i) == null ? "" : r.get(i)) : "";
                line.append("| ").append(pad(cell, widths[i])).append(" ");
            }
            line.append("|");
            System.out.println(line);
        }
        System.out.println(sep);
    }

    private static String pad(String s, int w) {
        if (s == null) s = "";
        if (s.length() >= w) return s;
        StringBuilder sb = new StringBuilder(s);
        while (sb.length() < w) sb.append(' ');
        return sb.toString();
    }

    private String formatTransactionDetails(Transaction tx) {
        if (tx == null || (tx.getSender().isEmpty() && tx.getReceiver().isEmpty())) {
            return "NO-OP";
        }

        if (tx.getSender().isEmpty() || tx.getReceiver().isEmpty()) {
            return "Incomplete transaction";
        }

        return String.format("%s → %s: $%d",
                tx.getSender(), tx.getReceiver(), tx.getAmount());
    }

    private String normalizePhaseSymbol(String s) {
        if (s == null || s.isEmpty()) return "X";
        char c = Character.toUpperCase(s.charAt(0));
        return switch (c) {
            case 'A', 'C', 'E', 'X' -> String.valueOf(c);
            default -> "X";
        };
    }

    private String getPhaseDescription(String phase) {
        return switch (phase) {
            case "A" -> "Accepted";
            case "C" -> "Committed";
            case "E" -> "Executed";
            case "X" -> "None";
            default -> "Unknown";
        };
    }


    public void verifyDatabaseConsistency() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("DATABASE CONSISTENCY CHECK");
        System.out.println("=".repeat(80));

        Map<String, String> databases = new LinkedHashMap<>();


        for (Map.Entry<String, NodeEndpoint> entry : endpointsByAlias.entrySet()) {
            try {
                DBDump dump = entry.getValue().printDB(1000);
                databases.put(entry.getKey(), dump.getContent());
            } catch (Exception ex) {
                LOG.warn("Could not retrieve DB from node {}: {}", entry.getKey(), ex.getMessage());
            }
        }

        if (databases.isEmpty()) {
            System.out.println("No databases retrieved!");
            System.out.println("=".repeat(80) + "\n");
            return;
        }


        String reference = null;
        String referenceNode = null;
        boolean consistent = true;

        for (Map.Entry<String, String> entry : databases.entrySet()) {
            if (reference == null) {
                reference = entry.getValue();
                referenceNode = entry.getKey();
            } else {
                if (!reference.equals(entry.getValue())) {
                    consistent = false;
                    System.out.printf("INCONSISTENCY: Node %s differs from Node %s\n",
                            entry.getKey(), referenceNode);
                }
            }
        }

        if (consistent) {
            System.out.println("✓ All node databases are CONSISTENT");
            System.out.println("\nCommon state:");
            System.out.println(reference);
        } else {
            System.out.println("✗ Database INCONSISTENCY detected!");
            System.out.println("\nIndividual node states:");
            for (Map.Entry<String, String> entry : databases.entrySet()) {
                System.out.printf("\nNode %s:\n%s\n", entry.getKey(), entry.getValue());
            }
        }

        System.out.println("=".repeat(80) + "\n");
    }

    @Override
    public void close() {
        Set<NodeEndpoint> unique = ConcurrentHashMap.newKeySet();
        unique.addAll(endpointsByAlias.values());
        unique.addAll(endpointsByTarget.values());
        unique.addAll(endpointsByNumericId.values());
        for (NodeEndpoint endpoint : unique) {
            try {
                endpoint.shutdown();
            } catch (Exception ex) {
                LOG.warn("Error shutting down channel for {}", endpoint.nodeId, ex);
            }
        }
    }

    private NodeEndpoint newEndpoint(String nodeId, String target) {
        ManagedChannel channel = NettyChannelBuilder.forTarget(target)
                .usePlaintext()
                .keepAliveTime(CLIENT_KEEPALIVE_TIME_SEC, TimeUnit.SECONDS)
                .keepAliveTimeout(CLIENT_KEEPALIVE_TIMEOUT_SEC, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(CLIENT_KEEPALIVE_WITHOUT_CALLS)
                .maxInboundMessageSize(CLIENT_MAX_INBOUND_MESSAGE_MB * 1024 * 1024)
                .flowControlWindow(CLIENT_FLOW_CONTROL_WINDOW_BYTES)
                .withOption(ChannelOption.TCP_NODELAY, CLIENT_TCP_NODELAY)
                .build();
        NodeServiceGrpc.NodeServiceBlockingStub stub = NodeServiceGrpc.newBlockingStub(channel);
        if (CLIENT_ENABLE_GZIP) {
            stub = stub.withCompression("gzip");
        }
        int numericId = parseNumericId(nodeId);
        return new NodeEndpoint(nodeId, target, channel, stub, numericId);
    }

    private int parseNumericId(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            return -1;
        }
        if (nodeId.indexOf(':') >= 0) {
            return -1;
        }
        String digits = nodeId.replaceAll("\\D", "");
        if (digits.isEmpty() || digits.length() > 9) {
            return -1;
        }
        try {
            return Integer.parseInt(digits);
        } catch (NumberFormatException ex) {
            return -1;
        }
    }

    public static final class NodeEndpoint {
        private final String nodeId;
        private final String target;
        private final ManagedChannel channel;
        private final NodeServiceGrpc.NodeServiceBlockingStub stub;
        private final int numericId;

        private NodeEndpoint(String nodeId, String target, ManagedChannel channel, NodeServiceGrpc.NodeServiceBlockingStub stub, int numericId) {
            this.nodeId = nodeId;
            this.target = target;
            this.channel = channel;
            this.stub = stub;
            this.numericId = numericId;
        }

        public String nodeId() {
            return nodeId;
        }

        public String target() {
            return target;
        }

        public OptionalInt numericId() {
            return numericId > 0 ? OptionalInt.of(numericId) : OptionalInt.empty();
        }

        public ClientReply submit(ClientRequest request, long timeoutMs) {
            NodeServiceGrpc.NodeServiceBlockingStub call = maybeCompressClientStub(stub, request.getSerializedSize())
                    .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
            return call.submit(request);
        }

        public String describe() {
            return nodeId.equals(target) ? target : nodeId + "(" + target + ")";
        }

        public void setActive(boolean active) {
            NodeControl request = NodeControl.newBuilder()
                    .setActive(active)
                    .setIsLeaderFailure(false)
                    .build();
            Exception last = null;
            for (int attempt = 0; attempt < 3; attempt++) {
                try {
                    stub.withDeadlineAfter(1200, TimeUnit.MILLISECONDS).control(request);
                    return;
                } catch (Exception ex) {
                    last = ex;
                    try { Thread.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                }
            }
            if (last != null) throw new RuntimeException(last);
        }

        private void shutdown() {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(250, TimeUnit.MILLISECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }

        public LogDump printLog(long timeoutMs) {
            return stub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                    .printLog(Empty.getDefaultInstance());
        }

        public DBDump printDB(long timeoutMs) {
            return stub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                    .printDB(Empty.getDefaultInstance());
        }

        public BalanceReply getBalance(int id, long timeoutMs) {
            BalanceRequest req = BalanceRequest.newBuilder().setId(id).build();
            return stub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).getBalance(req);
        }

        public PrintViewDump printView(long timeoutMs) {
            return stub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                    .printView(Empty.getDefaultInstance());
        }

        public StatusDump printStatus(long seqNum, long timeoutMs) {
            StatusRequest request = StatusRequest.newBuilder()
                    .setS(SeqNo.newBuilder().setValue(seqNum))
                    .build();
            return stub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                    .printStatus(request);
        }
    }
}
