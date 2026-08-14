package org.paxos.client;

import java.util.Random;

final class ZipfSampler {
    private final int min;
    private final int max;
    private final double s;
    private final double[] cdf;
    private final Random rng;
    private final int n;

    ZipfSampler(int minInclusive, int maxInclusive, double skew, long seed) {
        if (minInclusive > maxInclusive) throw new IllegalArgumentException("min>max");
        this.min = minInclusive;
        this.max = maxInclusive;
        this.n = (maxInclusive - minInclusive + 1);
        // Map skew [0,1] -> exponent s roughly [0.2, 1.5]
        this.s = Math.max(0.2, Math.min(1.5, 0.2 + 1.3 * skew));
        this.cdf = new double[n];
        this.rng = new Random(seed);
        buildCdf();
    }

    private void buildCdf() {
        double sum = 0.0;
        for (int i = 1; i <= n; i++) {
            sum += 1.0 / Math.pow(i, s);
        }
        double acc = 0.0;
        for (int i = 1; i <= n; i++) {
            acc += (1.0 / Math.pow(i, s)) / sum;
            cdf[i - 1] = acc;
        }
        cdf[n - 1] = 1.0;
    }

    int next() {
        double u = rng.nextDouble();
        // binary search
        int lo = 0, hi = n - 1;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (u <= cdf[mid]) hi = mid; else lo = mid + 1;
        }
        return min + lo;
    }

}
