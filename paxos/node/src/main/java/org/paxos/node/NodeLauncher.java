package org.paxos.node;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class NodeLauncher {
    private static final long SERVER_KEEPALIVE_TIME_SEC = Long.parseLong(
            System.getenv().getOrDefault("PAXOS_SERVER_KEEPALIVE_TIME_SEC", "30"));
    private static final long SERVER_KEEPALIVE_TIMEOUT_SEC = Long.parseLong(
            System.getenv().getOrDefault("PAXOS_SERVER_KEEPALIVE_TIMEOUT_SEC", "10"));
    private static final boolean SERVER_KEEPALIVE_WITHOUT_CALLS = Boolean.parseBoolean(
            System.getenv().getOrDefault("PAXOS_SERVER_KEEPALIVE_WITHOUT_CALLS", "true"));
    private static final int SERVER_MAX_INBOUND_MESSAGE_MB = Integer.parseInt(
            System.getenv().getOrDefault("PAXOS_SERVER_MAX_INBOUND_MESSAGE_MB", "8"));
    private static final int SERVER_FLOW_CONTROL_WINDOW_BYTES = Integer.parseInt(
            System.getenv().getOrDefault("PAXOS_SERVER_FLOW_CONTROL_WINDOW_BYTES", "1048576"));
    private static final boolean SERVER_TCP_NODELAY = Boolean.parseBoolean(
            System.getenv().getOrDefault("PAXOS_SERVER_TCP_NODELAY", "true"));
    private static class PeerHost {
        final String id;
        final String host;
        final int port;

        PeerHost(String id, String host, int port) {
            this.id = id;
            this.host = host;
            this.port = port;
        }

        String target() {
            return host + ":" + port;
        }
    }

    private static String determineAdvertisedHost(String[] args) throws IOException {
        if (args.length >= 4 && !args[3].isBlank()) {
            return args[3];
        }
        String envHost = System.getenv("PAXOS_NODE_HOST");
        if (envHost != null && !envHost.isBlank()) {
            return envHost;
        }
        return InetAddress.getLocalHost().getHostAddress();
    }

    private static Server startServer(String nodeId, NodeServiceImpl service, int port) throws IOException {
        NettyServerBuilder builder = NettyServerBuilder.forPort(port)
                .addService(service)
                .keepAliveTime(SERVER_KEEPALIVE_TIME_SEC, TimeUnit.SECONDS)
                .keepAliveTimeout(SERVER_KEEPALIVE_TIMEOUT_SEC, TimeUnit.SECONDS)
                .permitKeepAliveTime(30, TimeUnit.SECONDS)
                .permitKeepAliveWithoutCalls(SERVER_KEEPALIVE_WITHOUT_CALLS)
                .maxInboundMessageSize(SERVER_MAX_INBOUND_MESSAGE_MB * 1024 * 1024)
                .flowControlWindow(SERVER_FLOW_CONTROL_WINDOW_BYTES)
                .withChildOption(ChannelOption.TCP_NODELAY, SERVER_TCP_NODELAY);

        Server server = builder.build();
        server.start();
        System.out.printf("[%s] gRPC server started on port %d%n", nodeId, port);
        return server;
    }

    private static int numericNodeId(String nodeId) {
        String digits = nodeId.replaceAll("\\D", "");
        if (digits.isEmpty()) {
            throw new IllegalArgumentException("Node id must contain numeric digits: " + nodeId);
        }
        return Integer.parseInt(digits);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: NodeLauncher <nodeId> <port> <peersCSV> [advertiseHost]");
            System.err.println("Example: n2=127.0.0.1:50052,n3=127.0.0.1:50053");
            System.exit(2);
        }
        String nodeId = args[0];
        int port = Integer.parseInt(args[1]);

        deleteShardMap();
        String advertisedHost = determineAdvertisedHost(args);

        Map<String, PeerHost> peers = new LinkedHashMap<>();
        for (String part : args[2].split(",")) {
            String entry = part.trim();
            if (entry.isEmpty()) {
                continue;
            }

            String[] idAndEndpoint = entry.split("=", 2);
            if (idAndEndpoint.length != 2 || idAndEndpoint[0].isBlank()) {
                throw new IllegalArgumentException("Invalid peer entry: '" + entry + "' (expected id=host:port)");
            }

            String[] hostAndPort = idAndEndpoint[1].trim().split(":", 2);
            if (hostAndPort.length != 2 || hostAndPort[0].isBlank() || hostAndPort[1].isBlank()) {
                throw new IllegalArgumentException("Invalid peer endpoint: '" + idAndEndpoint[1].trim() + "' (expected host:port)");
            }

            int peerPort;
            try {
                peerPort = Integer.parseInt(hostAndPort[1].trim());
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("Invalid port for peer '" + idAndEndpoint[0].trim() + "': " + hostAndPort[1], ex);
            }

            String peerId = idAndEndpoint[0].trim();
            peers.put(peerId, new PeerHost(peerId, hostAndPort[0].trim(), peerPort));
        }

        List<String> peerTargets = peers.values().stream()
                .map(PeerHost::target)
                .collect(Collectors.toList());

        Map<Integer, String> addressBook = new LinkedHashMap<>();
        String selfTarget = advertisedHost + ":" + port;
        addressBook.put(numericNodeId(nodeId), selfTarget);
        for (PeerHost peer : peers.values()) {
            addressBook.put(numericNodeId(peer.id), peer.target());
        }
        String fullTopo = System.getenv("PAXOS_FULL_TOPOLOGY");
        if (fullTopo != null && !fullTopo.isBlank()) {
            for (String entry : fullTopo.split(",")) {
                String e = entry.trim();
                if (e.isEmpty()) continue;
                String[] idAndEndpoint = e.split("=", 2);
                if (idAndEndpoint.length != 2) continue;
                String peerId = idAndEndpoint[0].trim();
                String[] hostAndPort = idAndEndpoint[1].trim().split(":", 2);
                if (hostAndPort.length != 2) continue;
                try {
                    int pid = numericNodeId(peerId);
                    String target = hostAndPort[0].trim() + ":" + hostAndPort[1].trim();
                    addressBook.put(pid, target);
                } catch (IllegalArgumentException ignored) {
                }
            }
        }

        String autoTopo = System.getenv().getOrDefault("PAXOS_AUTO_FULL_TOPOLOGY", "true");
        if (Boolean.parseBoolean(autoTopo)) {
            for (int i = 1; i <= 9; i++) {
                addressBook.putIfAbsent(i, advertisedHost + ":" + (51050 + i));
            }
        }

        NodeState state = new NodeState(numericNodeId(nodeId), peerTargets, addressBook);
        NodeServiceImpl service = NodeServiceImpl.withTargets(state, peerTargets, true);

        Server server = startServer(nodeId, service, port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            service.shutdown();
            server.shutdownNow();
        }));

        Thread.sleep(1000);

        startTerminal(state, service);

        server.awaitTermination();
        service.shutdown();
    }

    private static void deleteShardMap() {
        try {
            java.nio.file.Path[] paths = new java.nio.file.Path[] {
                java.nio.file.Path.of("client", "shard-map.json"),
                java.nio.file.Path.of("client", "shard-map.new.json"),
                java.nio.file.Path.of("client", "client", "shard-map.json"),
                java.nio.file.Path.of("client", "client", "shard-map.new.json"),
                java.nio.file.Path.of("..", "client", "shard-map.json"),
                java.nio.file.Path.of("..", "client", "shard-map.new.json"),
                java.nio.file.Path.of("shard-map.json"),
                java.nio.file.Path.of("shard-map.new.json"),
                java.nio.file.Path.of("..", "shard-map.json"),
                java.nio.file.Path.of("..", "shard-map.new.json")
            };
            for (java.nio.file.Path p : paths) {
                if (java.nio.file.Files.exists(p)) {
                    try { java.nio.file.Files.delete(p); } catch (Exception ignore) {}
                }
            }
        } catch (Exception ignore) {}
    }

    private static void startTerminal(NodeState state, NodeServiceImpl service) {
        if (!Boolean.parseBoolean(System.getenv().getOrDefault("PAXOS_ENABLE_CONSOLE", "true"))) {
            return;
        }
        Thread console = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                while (true) {
                    System.out.print("paxos> ");
                    System.out.flush();
                    String line = reader.readLine();
                    if (line == null) {
                        System.out.println("Console input closed; background server still running.");
                        break;
                    }
                    String trimmedLine = line.trim();
                    if (trimmedLine.isEmpty()) {
                        continue;
                    }
                    String[] parts = trimmedLine.split("\s+");
                    String command = parts[0].toLowerCase();
                    switch (command) {
                        case "log" -> System.out.print(state.formatLog());
                        case "db" -> System.out.print(state.formatDb());
                        case "status" -> {
                            if (parts.length < 2) {
                                System.out.println("usage: status <seq>");
                                break;
                            }
                            try {
                                long seq = Long.parseLong(parts[1]);
                                Map<Integer, NodeState.Phase> statuses = service.collectClusterStatus(seq);
                                state.nodeAddressBook.keySet().stream()
                                        .sorted()
                                        .forEach(id -> {
                                            NodeState.Phase phase = statuses.getOrDefault(id, NodeState.Phase.NONE);
                                            System.out.printf("node %d -> %c (%s)%n", id, phase.symbol(), phase.name());
                                        });
                            } catch (NumberFormatException ex) {
                                System.out.println("Invalid sequence number");
                            }
                        }
                        case "view" -> System.out.print(state.formatViews());
                        case "sleep" -> {
                            service.deactivate();
                            System.out.println("Node set to inactive (sleep).");
                        }
                        case "wake" -> {
                            service.activate();
                            System.out.println("Node reactivated (wake).");
                        }
                        case "help" -> System.out.println("Commands: log, db, status <seq>, view, sleep, wake, quit");
                        case "quit", "exit" -> {
                            System.out.println("Console loop exiting (server still running).");
                            return;
                        }
                        default -> System.out.println("Unknown command. Type 'help' for options.");
                    }
                }
            } catch (Exception ex) {
                System.out.printf("[node %d] Console error: %s%n", state.nodeNumericId, ex.getMessage());
            }
        }, "paxos-console");
        console.setDaemon(true);
        console.start();
    }
}
