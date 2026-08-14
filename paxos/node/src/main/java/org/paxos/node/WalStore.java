package org.paxos.node;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

final class WalStore {
    private DB db;
    private HTreeMap<String, NodeState.WalRecord> map;
    private final Path filePath;
    private final int nodeId;

    private static final boolean WAL_GROUP_COMMIT = Boolean.parseBoolean(
            System.getenv().getOrDefault("WAL_GROUP_COMMIT", "true"));
    private static final long WAL_COMMIT_INTERVAL_MS = Long.parseLong(
            System.getenv().getOrDefault("WAL_COMMIT_INTERVAL_MS", "5"));
    private static final int WAL_COMMIT_BATCH = Integer.parseInt(
            System.getenv().getOrDefault("WAL_COMMIT_BATCH", "64"));

    private final boolean groupCommit;
    private final long commitIntervalMs;
    private final int commitBatch;
    private final ScheduledExecutorService commitExec;
    private final AtomicInteger uncommittedOps = new AtomicInteger(0);

    WalStore(int nodeId) {
        this.nodeId = nodeId;
        Path dataDir = Paths.get("datastore");
        try {
            Files.createDirectories(dataDir);
        } catch (IOException ignore) {}
        this.filePath = dataDir.resolve(String.format("node-%d-wal.db", nodeId));
        this.db = DBMaker
                .fileDB(filePath.toString())
                .fileMmapEnableIfSupported()
                .transactionEnable()
                .closeOnJvmShutdown()
                .make();
        this.map = db.hashMap("wal", Serializer.STRING, Serializer.JAVA)
                .createOrOpen();
        this.groupCommit = WAL_GROUP_COMMIT;
        this.commitIntervalMs = Math.max(1L, WAL_COMMIT_INTERVAL_MS);
        this.commitBatch = Math.max(1, WAL_COMMIT_BATCH);
        if (groupCommit) {
            this.commitExec = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "wal-store-" + nodeId + "-commit");
                t.setDaemon(true);
                return t;
            });
            this.commitExec.scheduleAtFixedRate(this::backgroundCommitIfNeeded, commitIntervalMs, commitIntervalMs, TimeUnit.MILLISECONDS);
        } else {
            this.commitExec = null;
        }
    }

    synchronized void reset() {
        try { db.close(); } catch (RuntimeException ignore) {}
        try { Files.deleteIfExists(filePath); } catch (IOException ignore) {}
        this.db = DBMaker
                .fileDB(filePath.toString())
                .fileMmapEnableIfSupported()
                .transactionEnable()
                .closeOnJvmShutdown()
                .make();
        this.map = db.hashMap("wal", Serializer.STRING, Serializer.JAVA)
                .createOrOpen();
    }

    NodeState.WalRecord get(String key) {
        return map.get(key);
    }

    NodeState.WalRecord put(String key, NodeState.WalRecord value) {
        NodeState.WalRecord prev = map.put(key, value);
        commitMaybe();
        return prev;
    }

    NodeState.WalRecord remove(String key) {
        NodeState.WalRecord prev = map.remove(key);
        commitMaybe();
        return prev;
    }

    Collection<NodeState.WalRecord> values() {
        return map.values();
    }


    private void commitMaybe() {
        if (!groupCommit) {
            try { db.commit(); } catch (RuntimeException ignore) {}
            return;
        }
        int n = uncommittedOps.incrementAndGet();
        if (n >= commitBatch) {
            if (commitExec != null) {
                commitExec.execute(this::backgroundCommit);
            } else {
                backgroundCommit();
            }
        }
    }

    private void backgroundCommitIfNeeded() {
        if (uncommittedOps.get() > 0) {
            backgroundCommit();
        }
    }

    private void backgroundCommit() {
        int pending = uncommittedOps.getAndSet(0);
        if (pending <= 0) return;
        try {
            db.commit();
        } catch (RuntimeException ex) {
            try { db.rollback(); } catch (RuntimeException ignore) {}
        }
    }
}
