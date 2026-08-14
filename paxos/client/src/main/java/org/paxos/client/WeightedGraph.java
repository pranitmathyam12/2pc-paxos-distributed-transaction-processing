package org.paxos.client;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

final class WeightedGraph {
    static final class EdgeKey {
        final int u;
        final int v;
        EdgeKey(int a, int b) {
            if (a <= b) { this.u = a; this.v = b; }
            else { this.u = b; this.v = a; }
        }
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof EdgeKey)) return false;
            EdgeKey k = (EdgeKey) o;
            return u == k.u && v == k.v;
        }
        @Override public int hashCode() { return Objects.hash(u, v); }
    }

    private final Map<Integer, Long> vertexWeights = new HashMap<>();
    private final Map<EdgeKey, Long> edgeWeights = new HashMap<>();

    void addEdge(int a, int b, long w) {
        if (a <= 0 || b <= 0 || a == b) return;
        vertexWeights.compute(a, (k, old) -> old == null ? w : old + w);
        vertexWeights.compute(b, (k, old) -> old == null ? w : old + w);
        EdgeKey k = new EdgeKey(a, b);
        edgeWeights.compute(k, (ek, old) -> old == null ? w : old + w);
    }

    Map<Integer, Long> vertexWeights() { return Collections.unmodifiableMap(vertexWeights); }
    Map<EdgeKey, Long> edgeWeights() { return Collections.unmodifiableMap(edgeWeights); }
}
