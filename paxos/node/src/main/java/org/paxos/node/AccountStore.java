package org.paxos.node;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public final class AccountStore {

    private final DB db;
    private final HTreeMap<Integer, Long> accounts;
    private final int nodeId;
    private final Path filePath;
    private static final boolean ACCOUNT_DB_GROUP_COMMIT = Boolean.parseBoolean(
            System.getenv().getOrDefault("ACCOUNT_DB_GROUP_COMMIT", "true"));

    private static final long ACCOUNT_DB_COMMIT_INTERVAL_MS = Long.parseLong(
            System.getenv().getOrDefault("ACCOUNT_DB_COMMIT_INTERVAL_MS", "5"));

    private static final int ACCOUNT_DB_COMMIT_BATCH = Integer.parseInt(
            System.getenv().getOrDefault("ACCOUNT_DB_COMMIT_BATCH", "64"));

    @SuppressWarnings("unused")
    private static final boolean ACCOUNT_DB_FSYNC_DISABLE = Boolean.parseBoolean(
            System.getenv().getOrDefault("ACCOUNT_DB_FSYNC_DISABLE", "false"));

    private final boolean groupCommit;
    private final long commitIntervalMs;
    private final int commitBatch;
    private final ScheduledExecutorService commitExec;
    private final AtomicInteger uncommittedOps = new AtomicInteger(0);

    public AccountStore(int nodeId) {
        Path dataDir = Paths.get("datastore");
        try {
            Files.createDirectories(dataDir);
        } catch (IOException ignore) {
        }
        this.filePath = dataDir.resolve(String.format("node-%d-db.mapdb", nodeId));
        this.db = DBMaker
                .fileDB(filePath.toString())
                .fileMmapEnableIfSupported()
                .transactionEnable()
                .closeOnJvmShutdown()
                .make();
        this.nodeId = nodeId;
        this.accounts = db
                .hashMap("accounts", Serializer.INTEGER, Serializer.LONG)
                .createOrOpen();
        this.groupCommit = ACCOUNT_DB_GROUP_COMMIT;
        this.commitIntervalMs = Math.max(1L, ACCOUNT_DB_COMMIT_INTERVAL_MS);
        this.commitBatch = Math.max(1, ACCOUNT_DB_COMMIT_BATCH);
        if (groupCommit) {
            this.commitExec = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "acct-store-" + nodeId + "-commit");
                t.setDaemon(true);
                return t;
            });
            this.commitExec.scheduleAtFixedRate(this::backgroundCommitIfNeeded, commitIntervalMs, commitIntervalMs, TimeUnit.MILLISECONDS);
        } else {
            this.commitExec = null;
        }
        seedIfEmpty();
    }

    private void seedIfEmpty() {
        if (!accounts.isEmpty()) {
            return;
        }
        ShardMapper mapper = ShardMapper.loadOrDefault(nodeId);
        int myCluster = (nodeId - 1) / 3;
        for (int i = 1; i <= 9000; i++) {
            if (mapper.clusterIndexForAccountId(i) == myCluster) {
                accounts.put(i, 10L);
            }
        }
        db.commit();
    }

    public long getBalance(int id) {
        Long v = accounts.get(id);
        return v == null ? 0L : v;
    }

    public boolean setBalance(int id, long newBalance) {
        try {
            accounts.put(id, newBalance);
            commitMaybe();
            return true;
        } catch (RuntimeException ex) {
            db.rollback();
            return false;
        }
    }

    public Result transfer(int senderId, int receiverId, long amount) {
        if (amount < 0) {
            return Result.err("NEGATIVE_AMOUNT");
        }
        try {
            Long sBalObj = accounts.get(senderId);
            if (sBalObj == null) {
                accounts.put(senderId, 10L);
                commitMaybe();
                sBalObj = accounts.get(senderId);
                if (sBalObj == null) {
                    return Result.err("UNKNOWN_SENDER");
                }
            }
            long sBal = sBalObj;
            if (sBal < amount) {
                return Result.err("INSUFFICIENT_FUNDS");
            }
            Long rBalObj = accounts.get(receiverId);
            if (rBalObj == null) {
                accounts.put(receiverId, 10L);
                commitMaybe();
                rBalObj = accounts.get(receiverId);
                if (rBalObj == null) {
                    return Result.err("UNKNOWN_RECEIVER");
                }
            }
            long rBal = rBalObj;

            long newSBal = sBal - amount;
            long newRBal = rBal + amount;
            accounts.put(senderId, newSBal);
            accounts.put(receiverId, newRBal);
            commitMaybe();
            return Result.ok();
        } catch (RuntimeException ex) {
            db.rollback();
            return Result.err("DB_ERROR");
        }
    }

    public record Result(boolean success, String error) {
        public static Result ok() {
            return new Result(true, "");
        }
        public static Result err(String error) {
            return new Result(false, error == null ? "" : error);
        }
    }

    public void reset() {
        try {
            accounts.clear();
            ShardMapper mapper = ShardMapper.loadOrDefault(nodeId);
            int myCluster = (nodeId - 1) / 3;
            for (int i = 1; i <= 9000; i++) {
                if (mapper.clusterIndexForAccountId(i) == myCluster) {
                    accounts.put(i, 10L);
                }
            }
            db.commit();
        } catch (RuntimeException ex) {
            db.rollback();
            throw new IllegalStateException("DB_RESET_FAILED", ex);
        }
    }

    public void snapshotTo(Path dir, long executedIndex) {
        try {
            Files.createDirectories(dir);
        } catch (IOException ignore) {}
        try {
            db.commit();
        } catch (RuntimeException ignore) {}
        Path out = dir.resolve(String.format("node-%d-snap-%d.mapdb", nodeId, executedIndex));
        try {
            Files.copy(filePath, out, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ignore) {}
    }

    public java.util.Map<Integer, Long> readAllBalances() {
        java.util.Map<Integer, Long> m = new java.util.LinkedHashMap<>();
        for (java.util.Map.Entry<Integer, Long> e : accounts.entrySet()) {
            m.put(e.getKey(), e.getValue());
        }
        return m;
    }

    public void replaceAllBalances(java.util.Map<Integer, Long> balances) {
        accounts.clear();
        accounts.putAll(balances);
        db.commit();
    }

    private void commitMaybe() {
        if (!groupCommit) {
            db.commit();
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
