package org.paxos.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class ReshardPlanner {
    static Result plan(int n, double balanceTol, int seeds) {
        Path history = findExisting(
            Path.of("client", "history.jsonl"),
            Path.of("..", "client", "history.jsonl"),
            Path.of("history.jsonl")
        );
        if (history == null) history = Path.of("client", "history.jsonl");
        ShardMapper currentMap = ShardMapper.loadOrDefault();
        
        java.util.List<int[]> crossShardTxns = new java.util.ArrayList<>();
        java.util.Map<Integer, java.util.Map<Integer, Integer>> pairCount = new java.util.HashMap<>();
        int linesRead = 0;
        
        try {
            if (Files.exists(history)) {
                List<String> all = Files.readAllLines(history, StandardCharsets.UTF_8);
                int from = Math.max(0, all.size() - n);
                List<String> window = all.subList(from, all.size());
                Pattern p = Pattern.compile("\\\"s\\\"\\s*:\\s*(\\d+).+?\\\"r\\\"\\s*:\\s*(\\d+)", Pattern.DOTALL);
                for (String line : window) {
                    Matcher m = p.matcher(line);
                    if (m.find()) {
                        int s = Integer.parseInt(m.group(1));
                        int r = Integer.parseInt(m.group(2));
                        linesRead++;
                        int cs = currentMap.clusterIndexForAccountId(s);
                        int cr = currentMap.clusterIndexForAccountId(r);
                        if (cs != cr) {
                            crossShardTxns.add(new int[]{s, r, cs, cr});
                            int a = Math.min(s, r);
                            int b = Math.max(s, r);
                            pairCount.computeIfAbsent(a, k -> new java.util.HashMap<>())
                                     .merge(b, 1, Integer::sum);
                        }
                    }
                }
            }
        } catch (IOException ignore) {}

        java.util.Map<Integer, Integer> overrides = new java.util.LinkedHashMap<>();
        java.util.Map<Integer, Integer> currentAssign = new java.util.HashMap<>();
        
        for (int[] tx : crossShardTxns) {
            int s = tx[0], r = tx[1], cs = tx[2], cr = tx[3];
            currentAssign.putIfAbsent(s, cs);
            currentAssign.putIfAbsent(r, cr);
        }

        int[] clusterSizes = new int[3];
        for (int id = 1; id <= 9000; id++) {
            int c = overrides.containsKey(id) ? overrides.get(id) : currentMap.clusterIndexForAccountId(id);
            clusterSizes[c]++;
        }
        int maxPerCluster = (int) Math.ceil(9000 / 3.0 * (1.0 + balanceTol));

        for (int[] tx : crossShardTxns) {
            int s = tx[0], r = tx[1];
            int cs = overrides.containsKey(s) ? overrides.get(s) : currentMap.clusterIndexForAccountId(s);
            int cr = overrides.containsKey(r) ? overrides.get(r) : currentMap.clusterIndexForAccountId(r);
            
            if (cs == cr) continue;
            
            boolean canMoveS = clusterSizes[cr] < maxPerCluster;
            boolean canMoveR = clusterSizes[cs] < maxPerCluster;
            
            int sDefault = defaultClusterForId(s);
            int rDefault = defaultClusterForId(r);
            boolean sAlreadyMoved = overrides.containsKey(s);
            boolean rAlreadyMoved = overrides.containsKey(r);
            
            if (canMoveS && !sAlreadyMoved && cs == sDefault) {
                overrides.put(s, cr);
                clusterSizes[cs]--;
                clusterSizes[cr]++;
            } else if (canMoveR && !rAlreadyMoved && cr == rDefault) {
                overrides.put(r, cs);
                clusterSizes[cr]--;
                clusterSizes[cs]++;
            } else if (canMoveS && !sAlreadyMoved) {
                overrides.put(s, cr);
                clusterSizes[cs]--;
                clusterSizes[cr]++;
            } else if (canMoveR && !rAlreadyMoved) {
                overrides.put(r, cs);
                clusterSizes[cr]--;
                clusterSizes[cs]++;
            }
        }

        String json = buildMappingJson(overrides);
        Path out = findWritablePath(
            Path.of("client", "shard-map.new.json"),
            Path.of("..", "client", "shard-map.new.json"),
            Path.of("shard-map.new.json")
        );
        try {
            Files.writeString(out, json, StandardCharsets.UTF_8);
        } catch (IOException ignore) {}
        return new Result(out.toAbsolutePath().toString(), linesRead, overrides.size());
    }

    private static String buildMappingJson(Map<Integer, Integer> accounts) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"epoch\": 1,\n");
        sb.append("  \"k\": 3,\n");
        sb.append("  \"clusters\": [\"c1\", \"c2\", \"c3\"],\n");
        sb.append("  \"assignment\": {\n");
        sb.append("    \"ranges\": [\n");
        sb.append("      {\"start\":1,\"end\":3000,\"cluster\":\"c1\"},\n");
        sb.append("      {\"start\":3001,\"end\":6000,\"cluster\":\"c2\"},\n");
        sb.append("      {\"start\":6001,\"end\":9000,\"cluster\":\"c3\"}\n");
        sb.append("    ],\n");
        sb.append("    \"accounts\": {\n");
        boolean first = true;
        for (Map.Entry<Integer, Integer> e : accounts.entrySet()) {
            if (!first) sb.append(",\n");
            first = false;
            int idx = Math.max(0, Math.min(2, e.getValue()));
            sb.append("      \"").append(e.getKey()).append("\": \"c").append(idx + 1).append("\"");
        }
        sb.append("\n    }\n");
        sb.append("  },\n");
        sb.append("  \"balance\": { \"tolerance\": 0.05 }\n");
        sb.append("}\n");
        return sb.toString();
    }

    private static int defaultClusterForId(int id) {
        if (id >= 1 && id <= 3000) return 0;
        if (id >= 3001 && id <= 6000) return 1;
        return 2;
    }

    record Result(String path, int transactionsRead, int accountsConsidered) {}

    private static Path findExisting(Path... candidates) {
        for (Path p : candidates) {
            if (Files.exists(p)) return p;
        }
        return null;
    }

    private static Path findWritablePath(Path... candidates) {
        for (Path p : candidates) {
            Path parent = p.getParent();
            if (parent == null || Files.isDirectory(parent)) return p;
        }
        return candidates[0];
    }
}
