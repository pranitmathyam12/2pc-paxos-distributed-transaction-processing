package org.paxos.client;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class ShardMapper {
    private final int k;
    private final Map<Integer, Integer> accountToCluster;
    private final List<Range> ranges;

    private ShardMapper(int k, Map<Integer, Integer> accountToCluster, List<Range> ranges) {
        this.k = k;
        this.accountToCluster = accountToCluster;
        this.ranges = ranges;
    }

    int k() { return k; }

    int clusterIndexForAccountId(int id) {
        Integer idx = accountToCluster.get(id);
        if (idx != null) return idx;
        for (Range r : ranges) {
            if (id >= r.start && id <= r.end) return r.clusterIdx;
        }
        if (id >= 1 && id <= 3000) return 0;
        if (id >= 3001 && id <= 6000) return 1;
        return 2;
    }

    static ShardMapper loadOrDefault() {
        try {
            Path[] candidates = new Path[] {
                    Path.of("client", "shard-map.json"),
                    Path.of("..", "client", "shard-map.json"),
                    Path.of("shard-map.json"),
                    Path.of("..", "shard-map.json"),
                    Path.of("client", "client", "shard-map.json")
            };
            Path chosen = null;
            for (Path c : candidates) { if (Files.exists(c)) { chosen = c; break; } }
            if (chosen == null) return defaultRanges();
            String json = Files.readString(chosen);
            return parseJsonOrDefault(json);
        } catch (Exception ex) {
            return defaultRanges();
        }
    }

    private static ShardMapper defaultRanges() {
        List<Range> rs = new ArrayList<>();
        rs.add(new Range(1, 3000, 0));
        rs.add(new Range(3001, 6000, 1));
        rs.add(new Range(6001, 9000, 2));
        return new ShardMapper(3, new HashMap<>(), rs);
    }

    private static ShardMapper parseJsonOrDefault(String json) {
        if (json == null || json.isBlank()) return defaultRanges();
        int k = 3;
        Map<Integer, Integer> accounts = new HashMap<>();
        List<Range> rs = new ArrayList<>();

        Pattern clustersPat = Pattern.compile("\"clusters\"\s*:\s*\\[(.*?)\\]", Pattern.DOTALL);
        Matcher mClusters = clustersPat.matcher(json);
        if (mClusters.find()) {
            String body = mClusters.group(1);
            int count = 0;
            Matcher mc = Pattern.compile("\"(.*?)\"").matcher(body);
            while (mc.find()) count++;
            if (count > 0) k = count;
        }

        Pattern accountsPat = Pattern.compile("\"accounts\"\s*:\s*\\{(.*?)\\}", Pattern.DOTALL);
        Matcher mAcc = accountsPat.matcher(json);
        if (mAcc.find()) {
            String body = mAcc.group(1);
            Matcher mp = Pattern.compile("\"(\\d+)\"\s*:\s*\"c(\\d+)\"").matcher(body);
            while (mp.find()) {
                int id = Integer.parseInt(mp.group(1));
                int c = Integer.parseInt(mp.group(2));
                int idx = Math.max(0, c - 1);
                accounts.put(id, idx);
            }
        }

        Pattern rangesPat = Pattern.compile("\"ranges\"\s*:\s*\\[(.*?)\\]", Pattern.DOTALL);
        Matcher mRanges = rangesPat.matcher(json);
        if (mRanges.find()) {
            String body = mRanges.group(1);
            Matcher mr = Pattern.compile("\\{[^}]*?\"start\"\s*:\s*(\\d+)\s*,[^}]*?\"end\"\s*:\s*(\\d+)\s*,[^}]*?\"cluster\"\s*:\s*\"c(\\d+)\"[^}]*?}"
            ).matcher(body);
            while (mr.find()) {
                int s = Integer.parseInt(mr.group(1));
                int e = Integer.parseInt(mr.group(2));
                int c = Integer.parseInt(mr.group(3));
                int idx = Math.max(0, c - 1);
                rs.add(new Range(s, e, idx));
            }
        }

        if (rs.isEmpty()) {
            rs.add(new Range(1, 3000, 0));
            rs.add(new Range(3001, 6000, 1));
            rs.add(new Range(6001, 9000, 2));
        }
        return new ShardMapper(k, accounts, rs);
    }

    private static final class Range {
        final int start;
        final int end;
        final int clusterIdx;
        Range(int start, int end, int clusterIdx) {
            this.start = start;
            this.end = end;
            this.clusterIdx = clusterIdx;
        }
    }
}
