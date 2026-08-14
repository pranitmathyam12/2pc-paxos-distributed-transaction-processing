package org.paxos.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.CountDownLatch;


public final class ClientLauncher {

    private static final Logger LOG = LoggerFactory.getLogger(ClientLauncher.class);

    private static volatile NodeDirectory globalDirectory = null;
    private static final PerformanceRecorder PERF = new PerformanceRecorder();
    private static volatile ClientConfig GLOBAL_CONFIG = null;
    private static volatile boolean consoleStarted = false;
    private static volatile CountDownLatch pauseLatch = null;
    private static volatile Integer pauseSetNumber = null;

    private ClientLauncher() {}

    public static void main(String[] args) {
        deleteClientShardMap();
        ClientConfig config;
        try {
            config = ClientConfig.parse(args);
        } catch (IllegalArgumentException ex) {
            LOG.error("Invalid arguments: {}", ex.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Config parse error", ex);
            }
            return;
        }

        GLOBAL_CONFIG = config;

        List<CsvSetParser.TestSet> testSets;
        CsvSetParser parser = new CsvSetParser();
        try {
            if (config.bench()) {
                SmallBankGenerator.generateCsv(
                        1,
                        config.benchOps(),
                        config.benchReadOnlyPct(),
                        config.benchCrossPct(),
                        config.benchSkew(),
                        config.benchSeed(),
                        config.csvPath().toString()
                );
            }
            testSets = parser.parse(config.csvPath());
        } catch (IOException ex) {
            LOG.error("Failed to read CSV {}: {}", config.csvPath(), ex.getMessage());
            return;
        }

        if (testSets.isEmpty()) {
            LOG.warn("No test sets found in CSV {}", config.csvPath());
            return;
        }

        Map<String, ClientThread> clients = new LinkedHashMap<>();
        try (NodeDirectory directory = new NodeDirectory(config.nodeTargets())) {

            globalDirectory = directory;
            startCommandConsole();
            if (!config.auto()) {
                promptToStart(testSets.get(0).setNumber());
            }
            for (int i = 0; i < testSets.size(); i++) {
                CsvSetParser.TestSet set = testSets.get(i);
                LOG.info("=== Processing Set {} ({} transactions, live nodes {}) ===",
                        set.setNumber(), set.transactions().size(), set.liveNodes());

                PERF.reset();
                PERF.startSet(set.setNumber());


                directory.freezeAll();
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }


                directory.resetAllNodes();
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }


                directory.resetClientSideState();
                for (ClientThread worker : clients.values()) {
                    worker.resetForNewSet();
                }


                directory.applyNodeActivity(set.liveNodes());
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }

                runSet(set, clients, directory, config);

                LOG.info("Set {} complete", set.setNumber());

                if (!config.auto() && i < testSets.size() - 1) {
                    promptForContinue(set.setNumber());
                }
            }

            System.out.println("\n" + "=".repeat(80));
            System.out.println("All test sets complete.");
            System.out.println("You can use inspection commands (log, db, status, view, verify, perf)");
            System.out.println("Type 'help' for available commands, or 'quit' to exit.");
            System.out.println("=".repeat(80) + "\n");

            waitForQuit();

        } finally {
            clients.values().forEach(ClientThread::resumeAfterUserInput);
            clients.values().forEach(ClientThread::shutdown);
            globalDirectory = null;
        }
    }

    private static void deleteClientShardMap() {
        try {
            java.nio.file.Path[] paths = new java.nio.file.Path[] {
                java.nio.file.Path.of("client", "shard-map.json"),
                java.nio.file.Path.of("client", "shard-map.new.json"),
                java.nio.file.Path.of("client", "history.jsonl"),
                java.nio.file.Path.of("client", "client", "shard-map.json"),
                java.nio.file.Path.of("client", "client", "shard-map.new.json"),
                java.nio.file.Path.of("client", "client", "history.jsonl"),
                java.nio.file.Path.of("..", "client", "shard-map.json"),
                java.nio.file.Path.of("..", "client", "shard-map.new.json"),
                java.nio.file.Path.of("..", "client", "history.jsonl"),
                java.nio.file.Path.of("shard-map.json"),
                java.nio.file.Path.of("shard-map.new.json"),
                java.nio.file.Path.of("history.jsonl"),
                java.nio.file.Path.of("..", "shard-map.json"),
                java.nio.file.Path.of("..", "shard-map.new.json"),
                java.nio.file.Path.of("..", "history.jsonl")
            };
            for (java.nio.file.Path p : paths) {
                if (java.nio.file.Files.exists(p)) {
                    try { java.nio.file.Files.delete(p); } catch (Exception ignore) {}
                }
            }
        } catch (Exception ignore) {}
    }

    private static void runSet(CsvSetParser.TestSet set,
                               Map<String, ClientThread> clients,
                               NodeDirectory directory,
                               ClientConfig config) {

        Set<String> effectiveLiveNodes = new HashSet<>(set.liveNodes());

        List<CsvSetParser.CsvTransaction> batch = new ArrayList<>();

        for (CsvSetParser.TestItem item : set.items()) {
            if (item.isLeaderFailure()) {
                dispatchBatch(set, batch, clients, effectiveLiveNodes, config, directory);
                batch.clear();

                String failedLeader = directory.getCurrentLeaderNodeId();

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }

                directory.deactivateLeader();

                if (failedLeader != null) {
                    effectiveLiveNodes.remove(failedLeader);
                }

                try {
                    Thread.sleep(4000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            } else if (item.isBalance()) {
                dispatchBatch(set, batch, clients, effectiveLiveNodes, config, directory);
                batch.clear();
                Integer id = item.balanceId();
                if (id != null) {
                    processReadOnlyBalance(id, effectiveLiveNodes, directory);
                }
            } else if (item.isFail()) {
                dispatchBatch(set, batch, clients, effectiveLiveNodes, config, directory);
                batch.clear();
                String nodeId = item.failNode();
                if (nodeId != null) {
                    directory.endpointFor(nodeId).ifPresent(ep -> {
                        try { ep.setActive(false); } catch (Exception ignored) {}
                    });
                    effectiveLiveNodes.remove(nodeId);

                    try {
                        Thread.sleep(4000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            } else if (item.isRecover()) {
                dispatchBatch(set, batch, clients, effectiveLiveNodes, config, directory);
                batch.clear();
                String nodeId = item.recoverNode();
                if (nodeId != null) {
                    directory.endpointFor(nodeId).ifPresent(ep -> {
                        try { ep.setActive(true); } catch (Exception ignored) {}
                    });
                    effectiveLiveNodes.add(nodeId);

                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            } else {

                batch.add(item.transaction());
            }
        }

        dispatchBatch(set, batch, clients, effectiveLiveNodes, config, directory);
    }


    private static void dispatchBatch(CsvSetParser.TestSet set,
                                      List<CsvSetParser.CsvTransaction> batch,
                                      Map<String, ClientThread> clients,
                                      Set<String> actualLiveNodes,
                                      ClientConfig config,
                                      NodeDirectory directory) {
        if (batch.isEmpty()) {
            return;
        }
        if (config.bench()) {
            int poolSize = Math.max(1, config.benchClients());
            @SuppressWarnings("unchecked")
            List<CsvSetParser.CsvTransaction>[] buckets = new List[poolSize];
            for (int i = 0; i < poolSize; i++) buckets[i] = new ArrayList<>();
            for (CsvSetParser.CsvTransaction tx : batch) {
                String sender = tx.sender();
                int idx = Math.floorMod(sender.hashCode(), poolSize);
                buckets[idx].add(tx);
            }
            int used = 0;
            for (int i = 0; i < poolSize; i++) if (!buckets[i].isEmpty()) used++;
            CountDownLatch latch = new CountDownLatch(Math.max(1, used));
            for (int i = 0; i < poolSize; i++) {
                if (buckets[i].isEmpty()) continue;
                String workerId = "bench-" + (i + 1);
                ClientThread worker = clients.computeIfAbsent(workerId, id -> {
                    ClientThread w = new ClientThread(id, config, directory, PERF);
                    w.start();
                    return w;
                });
                worker.submitWork(set.setNumber(), buckets[i], actualLiveNodes, latch);
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while waiting for batch in set {}", set.setNumber());
            }
            return;
        }

        Map<String, List<CsvSetParser.CsvTransaction>> bySender = groupBySender(batch);
        CountDownLatch latch = new CountDownLatch(bySender.size());

        for (Map.Entry<String, List<CsvSetParser.CsvTransaction>> entry : bySender.entrySet()) {
            String senderId = entry.getKey();
            ClientThread worker = clients.computeIfAbsent(senderId, id -> {
                ClientThread w = new ClientThread(id, config, directory, PERF);
                w.start();
                return w;
            });
            worker.submitWork(set.setNumber(), entry.getValue(), actualLiveNodes, latch);
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while waiting for batch in set {}", set.setNumber());
        }
    }

    private static Map<String, List<CsvSetParser.CsvTransaction>> groupBySender(List<CsvSetParser.CsvTransaction> transactions) {
        Map<String, List<CsvSetParser.CsvTransaction>> grouped = new LinkedHashMap<>();
        for (CsvSetParser.CsvTransaction tx : transactions) {
            String sender = tx.sender();
            grouped.computeIfAbsent(sender, key -> new ArrayList<>()).add(tx);
        }
        return grouped;
    }

    private static void processReadOnlyBalance(int id,
                                               Set<String> effectiveLiveNodes,
                                               NodeDirectory directory) {
        int attempts = Math.max(1, Integer.parseInt(System.getenv().getOrDefault("CLIENT_BAL_READ_ATTEMPTS", "3")));
        long delayMs = Math.max(1L, Long.parseLong(System.getenv().getOrDefault("CLIENT_BAL_READ_RETRY_MS", "300")));
        for (int i = 0; i < attempts; i++) {
            java.util.Optional<NodeDirectory.NodeEndpoint> leader = directory.leaderEndpointForAccount(id, effectiveLiveNodes);
            if (leader.isEmpty()) {
                try { Thread.sleep(delayMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                continue;
            }
            NodeDirectory.NodeEndpoint ep = leader.get();
            try {
                var reply = ep.getBalance(id, 1000);
                System.out.println("balance(" + id + ") -> " + ep.nodeId() + " : " + reply.getBalance());
                return;
            } catch (Exception ex) {
                try { Thread.sleep(delayMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }

    private static void promptForContinue(int setNumber) {
        System.out.printf("Set %d complete. Press Enter to continue...%n", setNumber);
        pauseSetNumber = setNumber;
        CountDownLatch latch = new CountDownLatch(1);
        pauseLatch = latch;
        try {
            latch.await();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while waiting for user input after set {}", setNumber);
        } finally {
            pauseLatch = null;
            pauseSetNumber = null;
        }
    }

    private static void promptToStart(int setNumber) {
        System.out.printf("Ready to start Set %d. Press Enter to start...%n", setNumber);
        pauseSetNumber = setNumber;
        CountDownLatch latch = new CountDownLatch(1);
        pauseLatch = latch;
        try {
            latch.await();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while waiting to start set {}", setNumber);
        } finally {
            pauseLatch = null;
            pauseSetNumber = null;
        }
    }

    private static void startCommandConsole() {
        if (consoleStarted) {
            return;
        }
        consoleStarted = true;

        Thread consoleThread = new Thread(() -> {

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            System.out.println("\n" + "=".repeat(80));
            System.out.println("CLIENT COMMAND CONSOLE STARTED");
            System.out.println("=".repeat(80));
            System.out.println("Available commands:");
            System.out.println("  log          - Print transaction log from all nodes");
            System.out.println("  db           - Print database/balances from all nodes");
            System.out.println("  status <seq> - Print status of sequence number across all nodes");
            System.out.println("  view         - Print view history (leader elections) from all nodes");
            System.out.println("  perf         - Print throughput/latency metrics by set");
            System.out.println("  verify       - Verify database consistency across nodes");
            System.out.println("  help         - Show this help message");
            System.out.println("  quit         - Exit the program");
            System.out.println("=".repeat(80));
            System.out.println("You can enter commands at any time during or after test execution.");
            System.out.println("=".repeat(80) + "\n");

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                while (true) {
                    System.out.print("client> ");
                    System.out.flush();
                    String line = reader.readLine();

                    if (line == null) {
                        break;
                    }

                    String trimmed = line.trim();
                    if (trimmed.isEmpty()) {
                        CountDownLatch latchRef = pauseLatch;
                        if (latchRef != null) {
                            Integer setNum = pauseSetNumber;
                            if (setNum != null) {
                                System.out.println("Continuing set " + setNum + "...");
                            } else {
                                System.out.println("Continuing...");
                            }
                            latchRef.countDown();
                        }
                        continue;
                    }

                    String[] parts = trimmed.split("\\s+");
                    String command = parts[0].toLowerCase();

                    try {
                        NodeDirectory directory = globalDirectory;
                        if (directory == null) {
                            System.out.println("Error: Node directory not available");
                            continue;
                        }

                        switch (command) {
                            case "log" -> directory.printLogAllNodes();
                            case "db" -> directory.printDBAllNodes();
                            case "status" -> {
                                if (parts.length < 2) {
                                    System.out.println("Usage: status <sequence_number>");
                                    System.out.println("Example: status 5");
                                    break;
                                }
                                try {
                                    long seq = Long.parseLong(parts[1]);
                                    directory.printStatusAllNodes(seq);
                                } catch (NumberFormatException ex) {
                                    System.out.println("Invalid sequence number: " + parts[1]);
                                    System.out.println("Please provide a valid number.");
                                }
                            }
                            case "view" -> directory.printViewAllNodes();
                            case "perf" -> PERF.printReport();
                            case "reshard" -> {
                                if (parts.length < 2) {
                                    System.out.println("Usage: reshard plan [n] [tol] [seeds]");
                                    System.out.println("       reshard apply");
                                    System.out.println("  plan  : generate reshard plan from transaction history");
                                    System.out.println("  apply : promote shard-map.new.json and reset nodes");
                                    break;
                                }
                                if ("plan".equalsIgnoreCase(parts[1])) {
                                    int n = 100000;
                                    double tol = 0.05;
                                    int seeds = 10;
                                    if (parts.length >= 3) {
                                        try { n = Integer.parseInt(parts[2]); } catch (NumberFormatException ignore) {}
                                    }
                                    if (parts.length >= 4) {
                                        try { tol = Double.parseDouble(parts[3]); } catch (NumberFormatException ignore) {}
                                    }
                                    if (parts.length >= 5) {
                                        try { seeds = Integer.parseInt(parts[4]); } catch (NumberFormatException ignore) {}
                                    }
                                    ReshardPlanner.Result r = ReshardPlanner.plan(n, tol, seeds);
                                    System.out.printf("Reshard plan written: %s (read %d txns, %d accounts moved)\n", r.path(), r.transactionsRead(), r.accountsConsidered());
                                    System.out.println("Run 'reshard apply' to activate the new mapping.");
                                } else if ("apply".equalsIgnoreCase(parts[1])) {
                                    java.nio.file.Path[] newCandidates = {
                                        java.nio.file.Path.of("client", "shard-map.new.json"),
                                        java.nio.file.Path.of("..", "client", "shard-map.new.json"),
                                        java.nio.file.Path.of("shard-map.new.json"),
                                        java.nio.file.Path.of("client", "client", "shard-map.new.json")
                                    };
                                    java.nio.file.Path[] liveCandidates = {
                                        java.nio.file.Path.of("client", "shard-map.json"),
                                        java.nio.file.Path.of("..", "client", "shard-map.json"),
                                        java.nio.file.Path.of("shard-map.json"),
                                        java.nio.file.Path.of("client", "client", "shard-map.json")
                                    };
                                    java.nio.file.Path newMap = null;
                                    for (java.nio.file.Path p : newCandidates) {
                                        if (java.nio.file.Files.exists(p)) { newMap = p; break; }
                                    }
                                    if (newMap == null) {
                                        System.out.println("No reshard plan found. Run 'reshard plan' first.");
                                        break;
                                    }
                                    java.nio.file.Path liveMap = liveCandidates[0];
                                    for (int i = 0; i < newCandidates.length; i++) {
                                        if (newMap.equals(newCandidates[i])) { liveMap = liveCandidates[i]; break; }
                                    }
                                    try {
                                        if (liveMap.getParent() != null) java.nio.file.Files.createDirectories(liveMap.getParent());
                                        java.nio.file.Files.copy(newMap, liveMap, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                                        System.out.println("Promoted " + newMap + " -> " + liveMap);
                                    } catch (java.io.IOException ex) {
                                        System.out.println("Failed to copy shard map: " + ex.getMessage());
                                        break;
                                    }
                                    System.out.println("Resetting all nodes to apply new shard mapping...");
                                    directory.resetAllNodes();
                                    try { Thread.sleep(500); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                                    directory.resetClientSideState();
                                    System.out.println("Reshard applied. Nodes and client now use the new mapping.");
                                } else {
                                    System.out.println("Unknown reshard subcommand: " + parts[1]);
                                    System.out.println("Usage: reshard plan | reshard apply");
                                }
                            }
                            case "smallbank" -> {
                                if (parts.length < 2 || !"gen".equalsIgnoreCase(parts[1])) {
                                    if (parts.length >= 2 && "run".equalsIgnoreCase(parts[1])) {

                                        int ops = 10000;
                                        int ro = 50;
                                        int cross = 50;
                                        double skew = 0.0;
                                        int seed = 12345;
                                        if (parts.length >= 3) { try { ops = Integer.parseInt(parts[2]); } catch (NumberFormatException ignore) {} }
                                        if (parts.length >= 4) { try { ro = Integer.parseInt(parts[3]); } catch (NumberFormatException ignore) {} }
                                        if (parts.length >= 5) { try { cross = Integer.parseInt(parts[4]); } catch (NumberFormatException ignore) {} }
                                        if (parts.length >= 6) { try { skew = Double.parseDouble(parts[5]); } catch (NumberFormatException ignore) {} }
                                        if (parts.length >= 7) { try { seed = Integer.parseInt(parts[6]); } catch (NumberFormatException ignore) {} }

                                        String tempCsv = "client/sb_run.csv";
                                        SmallBankGenerator.Result genRes = SmallBankGenerator.generateCsv(1, ops, ro, cross, skew, seed, tempCsv);

                                        if (GLOBAL_CONFIG == null) {
                                            System.out.println("Error: client config unavailable; cannot run.");
                                            break;
                                        }
                                        if (directory == null) {
                                            System.out.println("Error: Node directory not available");
                                            break;
                                        }


                                        CsvSetParser parser = new CsvSetParser();
                                        java.util.List<CsvSetParser.TestSet> sets;
                                        try {
                                            sets = parser.parse(java.nio.file.Paths.get(tempCsv));
                                        } catch (Exception ex) {
                                            System.out.println("Failed to parse generated CSV: " + ex.getMessage());
                                            break;
                                        }
                                        if (sets.isEmpty()) {
                                            System.out.println("No sets generated to run.");
                                            break;
                                        }

                                        java.util.Map<String, ClientThread> clients = new java.util.LinkedHashMap<>();
                                        CsvSetParser.TestSet set = sets.get(0);


                                        runSet(set, clients, directory, GLOBAL_CONFIG);
                                        System.out.printf("SmallBank run complete. Planned/achieved mix: RO%% target=%d, Cross%% target=%d | Achieved RO%%=%.1f, Cross%%=%.1f\n",
                                                ro, cross, genRes.readOnlyPctAchieved(), genRes.crossPctAchieved());
                                        System.out.println("Performance:");
                                        PERF.printReport();

                                        clients.values().forEach(ClientThread::resumeAfterUserInput);
                                        clients.values().forEach(ClientThread::shutdown);
                                        break;
                                    }
                                    System.out.println("Usage: smallbank gen [set] [ops] [readOnly%] [cross%] [skew] [seed] [outfile]");
                                    System.out.println("  set        : set number to write (default 1)");
                                    System.out.println("  ops        : total operations (default 10000)");
                                    System.out.println("  readOnly%  : 0..100 (default 50)");
                                    System.out.println("  cross%     : 0..100 (default 50)");
                                    System.out.println("  skew       : 0..1 (default 0.0)");
                                    System.out.println("  seed       : int seed (default 12345)");
                                    System.out.println("  outfile    : path (default client/test.csv)");
                                    break;
                                }
                                int set = 1;
                                int ops = 10000;
                                int ro = 50;
                                int cross = 50;
                                double skew = 0.0;
                                int seed = 12345;
                                String out = "client/test.csv";
                                if (parts.length >= 3) { try { set = Integer.parseInt(parts[2]); } catch (NumberFormatException ignore) {} }
                                if (parts.length >= 4) { try { ops = Integer.parseInt(parts[3]); } catch (NumberFormatException ignore) {} }
                                if (parts.length >= 5) { try { ro = Integer.parseInt(parts[4]); } catch (NumberFormatException ignore) {} }
                                if (parts.length >= 6) { try { cross = Integer.parseInt(parts[5]); } catch (NumberFormatException ignore) {} }
                                if (parts.length >= 7) { try { skew = Double.parseDouble(parts[6]); } catch (NumberFormatException ignore) {} }
                                if (parts.length >= 8) { try { seed = Integer.parseInt(parts[7]); } catch (NumberFormatException ignore) {} }
                                if (parts.length >= 9) { out = parts[8]; }
                                SmallBankGenerator.Result r = SmallBankGenerator.generateCsv(set, ops, ro, cross, skew, seed, out);
                                System.out.printf("SmallBank CSV written: %s\n", r.path());
                                System.out.printf("Ops=%d RO=%d RW=%d Intra=%d Cross=%d Achieved(RO%%=%.1f, Cross%%=%.1f)\n",
                                        r.ops(), r.readOnly(), r.readWrite(), r.intra(), r.cross(), r.readOnlyPctAchieved(), r.crossPctAchieved());
                            }
                            case "balance" -> {
                                if (parts.length < 2) {
                                    System.out.println("Usage: balance <id>");
                                    break;
                                }
                                try {
                                    int id = Integer.parseInt(parts[1]);
                                    directory.printBalanceForId(id);
                                } catch (NumberFormatException ex) {
                                    System.out.println("Invalid id: " + parts[1]);
                                }
                            }
                            case "verify" -> directory.verifyDatabaseConsistency();
                            case "help" -> printHelp();
                            case "quit", "exit" -> {
                                System.out.println("Exiting client console...");
                                System.exit(0);
                            }
                            default -> {
                                System.out.println("Unknown command: '" + command + "'");
                                System.out.println("Type 'help' for available commands.");
                            }
                        }
                    } catch (Exception ex) {
                        System.out.println("Error executing command: " + ex.getMessage());
                        LOG.error("Command execution error", ex);
                    }
                }
            } catch (IOException ex) {
                LOG.error("Console error: {}", ex.getMessage());
            }
        }, "client-console");

        consoleThread.setDaemon(false);
        consoleThread.start();
    }


    // used chatGPT to generate this function
    private static void printHelp() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("CLIENT COMMAND CONSOLE - HELP");
        System.out.println("=".repeat(80));
        System.out.println();

        System.out.println("COMMAND: log");
        System.out.println("  Description: Print transaction log from all nodes");
        System.out.println("  Shows: Sequence number, phase (A/C/E/X), transaction details, execution result");
        System.out.println("  Usage: log");
        System.out.println();

        System.out.println("COMMAND: db");
        System.out.println("  Description: Print database/datastore from all nodes");
        System.out.println("  Shows: Current balances for numeric client accounts (1..9000) that changed on each node");
        System.out.println("  Usage: db");
        System.out.println();

        System.out.println("COMMAND: status <seq>");
        System.out.println("  Description: Print status of a specific sequence number across all nodes");
        System.out.println("  Shows: Which phase each node is at for the given transaction");
        System.out.println("  Phases: A=Accepted, C=Committed, E=Executed, X=No Status");
        System.out.println("  Usage: status <sequence_number>");
        System.out.println("  Example: status 5");
        System.out.println();

        System.out.println("COMMAND: view");
        System.out.println("  Description: Print view history (leader elections) from all nodes");
        System.out.println("  Shows: All new-view messages exchanged during leader changes");
        System.out.println("  Includes: Ballot numbers, leader IDs, and entries in each view");
        System.out.println("  Usage: view");
        System.out.println();

        System.out.println("COMMAND: perf");
        System.out.println("  Description: Print throughput and latency metrics by set (client-side RTT)");
        System.out.println("  Shows: Count, avg, P50, P95, P99, min, max, throughput (tps)");
        System.out.println("  Usage: perf");
        System.out.println();

        System.out.println("COMMAND: verify");
        System.out.println("  Description: Verify database consistency across all nodes");
        System.out.println("  Shows: Whether all nodes have identical database states");
        System.out.println("  Usage: verify");
        System.out.println();

        System.out.println("COMMAND: help");
        System.out.println("  Description: Show this help message");
        System.out.println("  Usage: help");
        System.out.println();

        System.out.println("COMMAND: quit / exit");
        System.out.println("  Description: Exit the client program");
        System.out.println("  Usage: quit");
        System.out.println();

        System.out.println("=".repeat(80) + "\n");
    }

    private static void waitForQuit() {
        try {
            Thread.currentThread().join();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}