package org.paxos.tools;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.paxos.node.ShardMapper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public final class OfflineMigrator {

    private record Store(int nodeId, DB db, HTreeMap<Integer, Long> map) {}

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: OfflineMigrator <new-shard-map.json> [<old-shard-map.json>]");
            System.out.println("Run with ALL nodes stopped. This tool rewrites datastore/node-*-db.mapdb files.");
            return;
        }
        Path newMapPath = Paths.get(args[0]);
        Path oldMapPath = args.length >= 2 ? Paths.get(args[1]) : null;

        ShardMapper newMap = loadMapper(newMapPath);
        ShardMapper oldMap = oldMapPath != null && Files.exists(oldMapPath)
                ? loadMapper(oldMapPath)
                : ShardMapper.loadOrDefault(1);

        System.out.println("Opening data stores...");
        List<Store> stores = new ArrayList<>();
        for (int nodeId = 1; nodeId <= 9; nodeId++) {
            stores.add(openStore(nodeId));
        }

        int moves = 0;
        long[] clusterSums = new long[3];
        long[] nodeSums = new long[10];

        System.out.println("Computing move-set and applying...");
        for (int id = 1; id <= 9000; id++) {
            int oldIdx = oldMap.clusterIndexForAccountId(id);
            int newIdx = newMap.clusterIndexForAccountId(id);
            long bal = readFromCluster(stores, oldIdx, id);
            if (oldIdx != newIdx) moves++;
            // write to all nodes in new cluster
            for (int n = newIdx * 3 + 1; n <= newIdx * 3 + 3; n++) {
                put(stores.get(n - 1), id, bal);
            }
            // remove from all nodes NOT in new cluster
            for (int c = 0; c < 3; c++) {
                if (c == newIdx) continue;
                for (int n = c * 3 + 1; n <= c * 3 + 3; n++) {
                    remove(stores.get(n - 1), id);
                }
            }
        }

        // Summaries
        for (int nodeId = 1; nodeId <= 9; nodeId++) {
            long sum = sumBalances(stores.get(nodeId - 1));
            nodeSums[nodeId] = sum;
            int c = (nodeId - 1) / 3;
            clusterSums[c] += sum;
        }

        System.out.println("Committing...");
        for (Store s : stores) s.db.commit();

        writeReport(moves, clusterSums, nodeSums);

        System.out.println("Done. Moved accounts: " + moves);
        for (Store s : stores) {
            try { s.db.close(); } catch (Exception ignore) {}
        }
    }

    private static ShardMapper loadMapper(Path p) throws IOException {
        String json = Files.readString(p, StandardCharsets.UTF_8);
        return ShardMapper.parseJson(json);
    }

    private static Store openStore(int nodeId) {
        String file = Paths.get("datastore").resolve(String.format("node-%d-db.mapdb", nodeId)).toString();
        DB db = DBMaker
                .fileDB(file)
                .fileMmapEnableIfSupported()
                .transactionEnable()
                .closeOnJvmShutdown()
                .make();
        HTreeMap<Integer, Long> map = db.hashMap("accounts", Serializer.INTEGER, Serializer.LONG).createOrOpen();
        return new Store(nodeId, db, map);
    }

    private static long readFromCluster(List<Store> stores, int clusterIdx, int id) {
        int nodeId = clusterIdx * 3 + 1;
        Long v = stores.get(nodeId - 1).map.get(id);
        return v == null ? 0L : v;
    }

    private static void put(Store s, int id, long bal) {
        s.map.put(id, bal);
    }

    private static void remove(Store s, int id) {
        s.map.remove(id);
    }

    private static long sumBalances(Store s) {
        long sum = 0L;
        for (Long v : s.map.values()) sum += (v == null ? 0L : v);
        return sum;
    }

    private static void writeReport(int moves, long[] clusterSums, long[] nodeSums) {
        StringBuilder sb = new StringBuilder();
        sb.append("Reshard Migration Report\n");
        sb.append("Moved accounts: ").append(moves).append('\n');
        for (int c = 0; c < 3; c++) {
            sb.append("Cluster ").append(c + 1).append(" sum=").append(clusterSums[c]).append('\n');
        }
        for (int n = 1; n <= 9; n++) {
            sb.append("Node ").append(n).append(" sum=").append(nodeSums[n]).append('\n');
        }
        try (BufferedWriter w = Files.newBufferedWriter(Paths.get("migration-report.txt"), StandardCharsets.UTF_8)) {
            w.write(sb.toString());
        } catch (IOException ignore) {}
    }
}
