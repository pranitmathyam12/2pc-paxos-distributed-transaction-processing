package org.paxos.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

final class Balanced3WayPartitioner {
    static Map<Integer, Integer> partition(WeightedGraph g, double balanceTol, int seeds) {
        return partition(g, balanceTol, seeds, java.util.Collections.emptyMap(), 0.0);
    }

    static Map<Integer, Integer> partition(WeightedGraph g,
                                           double balanceTol,
                                           int seeds,
                                           Map<Integer, Integer> current,
                                           double movePenalty) {
        Map<Integer, Long> vw = g.vertexWeights();
        if (vw.isEmpty()) return Collections.emptyMap();

        Map<WeightedGraph.EdgeKey, Long> ew = g.edgeWeights();
        Map<Integer, Map<Integer, Long>> adj = new HashMap<>();
        for (Map.Entry<WeightedGraph.EdgeKey, Long> e : ew.entrySet()) {
            int a = e.getKey().u;
            int b = e.getKey().v;
            long w = e.getValue();
            adj.computeIfAbsent(a, k -> new HashMap<>()).compute(b, (k, old) -> old == null ? w : old + w);
            adj.computeIfAbsent(b, k -> new HashMap<>()).compute(a, (k, old) -> old == null ? w : old + w);
        }

        long totalWeight = 0L;
        for (long w : vw.values()) totalWeight += w;
        double target = totalWeight / 3.0;
        double maxPer = target * (1.0 + Math.max(0.0, balanceTol));
        double minPer = Math.max(0.0, target * (1.0 - Math.max(0.0, balanceTol)));

        List<Integer> vertices = new ArrayList<>(vw.keySet());
        vertices.sort((a, b) -> Long.compare(vw.getOrDefault(b, 0L), vw.getOrDefault(a, 0L)));

        Map<Integer, Integer> best = null;
        long bestCut = Long.MAX_VALUE;

        Random rng = new Random(1337);
        int runs = Math.max(1, seeds);
        for (int run = 0; run < runs; run++) {

            double[] partWeight = new double[3];
            Map<Integer, Integer> part = new HashMap<>();
            List<Integer> order = new ArrayList<>(vertices);
            Collections.shuffle(order, rng);
            for (int v : order) {
                long wv = vw.getOrDefault(v, 0L);
                int bestIdx = 0;
                double bestScore = Double.POSITIVE_INFINITY;
                for (int p = 0; p < 3; p++) {
                    double newW = partWeight[p] + wv;
                    double overflow = Math.max(0.0, newW - maxPer);
                    double underflow = Math.max(0.0, minPer - newW);
                    double penalty = 0.0;
                    Integer cur = current.get(v);
                    if (cur != null && cur != p) {
                        penalty = movePenalty * wv;
                    }
                    double score = newW + overflow * 10 + underflow * 10 + penalty; 
                    if (score < bestScore) { bestScore = score; bestIdx = p; }
                }
                part.put(v, bestIdx);
                partWeight[bestIdx] += wv;
            }

            boolean improved = true;
            int guard = 0;
            while (improved && guard++ < 10_000) {
                improved = false;
                Collections.shuffle(order, rng);
                for (int v : order) {
                    int cur = part.get(v);
                    long wv = vw.getOrDefault(v, 0L);

                    int bestIdx = cur;
                    long bestGain = 0L;
                    for (int p = 0; p < 3; p++) {
                        if (p == cur) continue;
                        long gain = -deltaCutForMove(v, cur, p, part, adj);
                        Integer curAssign = current.get(v);
                        if (curAssign != null) {
                            double oldPen = (curAssign == cur) ? 0.0 : (movePenalty * wv);
                            double newPen = (curAssign == p) ? 0.0 : (movePenalty * wv);
                            gain -= (long) Math.round(newPen - oldPen);
                        }
                        if (gain > bestGain) {
                        
                            double[] pw = new double[3];
                            for (Map.Entry<Integer, Integer> e : part.entrySet()) {
                                pw[e.getValue()] += vw.getOrDefault(e.getKey(), 0L);
                            }
                            pw[cur] -= wv; pw[p] += wv;
                            if (pw[p] <= maxPer + 1e-6 && pw[cur] >= minPer - 1e-6) {
                                bestGain = gain;
                                bestIdx = p;
                            }
                        }
                    }
                    if (bestIdx != cur && bestGain > 0) {
                        part.put(v, bestIdx);
                        improved = true;
                    }
                }
            }

            long cut = cutWeight(part, adj);
            if (cut < bestCut) {
                bestCut = cut;
                best = part;
            }
        }

        return best == null ? Collections.emptyMap() : best;
    }

    private static long cutWeight(Map<Integer, Integer> part, Map<Integer, Map<Integer, Long>> adj) {
        long cut = 0L;
        for (Map.Entry<Integer, Map<Integer, Long>> e : adj.entrySet()) {
            int u = e.getKey();
            int pu = part.getOrDefault(u, 0);
            for (Map.Entry<Integer, Long> v : e.getValue().entrySet()) {
                int vv = v.getKey();
                if (u < vv) {
                    int pv = part.getOrDefault(vv, 0);
                    if (pu != pv) cut += v.getValue();
                }
            }
        }
        return cut;
    }

    private static long deltaCutForMove(int v, int from, int to, Map<Integer, Integer> part, Map<Integer, Map<Integer, Long>> adj) {
        if (from == to) return 0L;
        long delta = 0L;
        Map<Integer, Long> nbrs = adj.getOrDefault(v, Collections.emptyMap());
        for (Map.Entry<Integer, Long> e : nbrs.entrySet()) {
            int u = e.getKey();
            long w = e.getValue();
            int pu = part.getOrDefault(u, from);
            boolean wasCut = (pu != from);
            boolean nowCut = (pu != to);
            if (wasCut && !nowCut) delta -= w;
            else if (!wasCut && nowCut) delta += w;
        }
        return delta;
    }
}
