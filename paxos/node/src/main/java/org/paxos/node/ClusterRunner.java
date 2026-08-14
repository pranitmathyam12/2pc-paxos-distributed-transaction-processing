package org.paxos.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class ClusterRunner {

    private static final List<NodeSpec> TOPOLOGY = List.of(
            new NodeSpec("n1", 51051),
            new NodeSpec("n2", 51052),
            new NodeSpec("n3", 51053),
            new NodeSpec("n4", 51054),
            new NodeSpec("n5", 51055),
            new NodeSpec("n6", 51056),
            new NodeSpec("n7", 51057),
            new NodeSpec("n8", 51058),
            new NodeSpec("n9", 51059)
    );

    private ClusterRunner() {
    }

    public static void main(String[] args) throws Exception {
        String host = resolveHost();
        String javaBin = System.getProperty("java.home") + "/bin/java";
        String classpath = System.getProperty("java.class.path");

        List<Process> processes = new ArrayList<>();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> stopAll(processes)));

        for (NodeSpec spec : TOPOLOGY) {
            String peersCsv = buildPeersCsv(spec.id(), host);
            ProcessBuilder builder = new ProcessBuilder(
                    javaBin,
                    "-cp",
                    classpath,
                    "org.paxos.node.NodeLauncher",
                    spec.id(),
                    Integer.toString(spec.port()),
                    peersCsv,
                    host
            );
            builder.directory(null);
            builder.inheritIO();
            builder.environment().putIfAbsent("PAXOS_NODE_HOST", host);
            Process process = builder.start();
            processes.add(process);
            System.out.printf(Locale.ROOT, "Started %s on %s:%d%n", spec.id(), host, spec.port());
            Thread.sleep(150);
        }

        boolean autoMode = Boolean.parseBoolean(System.getenv().getOrDefault("PAXOS_CLUSTER_AUTO", "false"));
        if (autoMode) {
            System.out.println("All nodes launched (auto mode). Send SIGINT to terminate the cluster...");
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        } else {
            System.out.println("All nodes launched. Press ENTER to terminate the cluster...");
            try {
                while (System.in.read() != -1) {
                    break;
                }
            } catch (IOException ignored) {
            }
        }

        stopAll(processes);
    }

    private static void stopAll(List<Process> processes) {
        for (Process process : processes) {
            if (process == null) {
                continue;
            }
            process.destroy();
        }
        for (Process process : processes) {
            if (process == null) {
                continue;
            }
            try {
                process.waitFor();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static String resolveHost() {
        String env = System.getenv("PAXOS_NODE_HOST");
        if (env != null && !env.isBlank()) {
            return env.trim();
        }
        return "127.0.0.1";
    }

    private static String buildPeersCsv(String selfId, String host) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        int myCluster = clusterIndex(selfId);
        for (NodeSpec spec : TOPOLOGY) {
            if (spec.id().equals(selfId)) {
                continue;
            }
            if (clusterIndex(spec.id()) != myCluster) {
                continue;
            }
            if (!first) {
                sb.append(',');
            }
            sb.append(spec.id()).append('=').append(host).append(':').append(spec.port());
            first = false;
        }
        return sb.toString();
    }

    private static int clusterIndex(String nodeId) {
        String digits = nodeId.replaceAll("\\D", "");
        if (digits.isEmpty()) return -1;
        int n = Integer.parseInt(digits);
        return (n - 1) / 3;
    }

    private record NodeSpec(String id, int port) {
    }
}
