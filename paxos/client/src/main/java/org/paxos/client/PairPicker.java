package org.paxos.client;

import java.util.Random;

final class PairPicker {
    private final ShardMapper mapper;
    private final boolean useZipf;
    private final ZipfSampler zipf;
    private final UniformSampler uni;
    private final UniformSampler uniAll;
    private final Random rng;

    PairPicker(ShardMapper mapper, int minId, int maxId, double skew, long seed) {
        this.mapper = mapper;
        this.useZipf = skew > 0.0;
        this.zipf = useZipf ? new ZipfSampler(minId, maxId, skew, seed) : null;
        this.uni = new UniformSampler(minId, maxId, seed ^ 0x9E3779B97F4A7C15L);
        this.uniAll = new UniformSampler(minId, maxId, seed ^ 0xD1B54A32D192ED03L);
        this.rng = new Random(seed ^ 0xA0761D6478BD642FL);
    }

    int sampleId() {
        return useZipf ? zipf.next() : uni.next();
    }

    private int sampleIdInCluster(int clusterIdx) {

        for (int i = 0; i < 500; i++) {
            int id = sampleId();
            if (mapper.clusterIndexForAccountId(id) == clusterIdx) return id;
        }

        for (int i = 0; i < 5000; i++) {
            int id = uniAll.next();
            if (mapper.clusterIndexForAccountId(id) == clusterIdx) return id;
        }

        if (clusterIdx == 0) return 1 + rng.nextInt(3000);
        if (clusterIdx == 1) return 3001 + rng.nextInt(3000);
        return 6001 + rng.nextInt(3000);
    }

    int[] nextIntraPair() {
        int s = sampleId();
        int c = mapper.clusterIndexForAccountId(s);
        int r;
        int guard = 0;
        do {
            r = sampleIdInCluster(c);
            guard++;
        } while ((r == s) && guard < 1000);
        if (r == s) {

            r = sampleIdInCluster(c);
            if (r == s) r = sampleIdInCluster(c);
        }
        return new int[]{s, r};
    }

    int[] nextCrossPair() {
        int s = sampleId();
        int c = mapper.clusterIndexForAccountId(s);
        int r;
        int guard = 0;
        do {
            r = sampleId();
            guard++;
        } while ((mapper.clusterIndexForAccountId(r) == c || r == s) && guard < 2000);
        if (mapper.clusterIndexForAccountId(r) == c || r == s) {

            int target = (c + (rng.nextBoolean() ? 1 : 2)) % 3;
            r = sampleIdInCluster(target);
            if (r == s) r = sampleIdInCluster(target);
        }
        return new int[]{s, r};
    }
}
