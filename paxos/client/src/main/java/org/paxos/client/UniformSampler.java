package org.paxos.client;

import java.util.Random;

final class UniformSampler {
    private final int min;
    private final int n;
    private final Random rng;

    UniformSampler(int minInclusive, int maxInclusive, long seed) {
        if (minInclusive > maxInclusive) throw new IllegalArgumentException("min>max");
        this.min = minInclusive;
        this.n = (maxInclusive - minInclusive + 1);
        this.rng = new Random(seed);
    }

    int next() {
        return min + rng.nextInt(n);
    }
}
