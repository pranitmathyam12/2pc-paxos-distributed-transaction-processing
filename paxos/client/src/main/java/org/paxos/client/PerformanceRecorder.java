package org.paxos.client;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

final class PerformanceRecorder {
    private static final class SetStats {
        final int setNumber;
        final List<Long> latenciesNs = Collections.synchronizedList(new ArrayList<>());
        final LongAdder count = new LongAdder();
        final LongAdder sumNs = new LongAdder();
        volatile long minNs = Long.MAX_VALUE;
        volatile long maxNs = 0L;
        volatile long firstStartNs = Long.MAX_VALUE;
        volatile long lastEndNs = 0L;

        SetStats(int setNumber) { this.setNumber = setNumber; }

        void record(long startNs, long endNs) {
            long lat = Math.max(0L, endNs - startNs);
            latenciesNs.add(lat);
            count.increment();
            sumNs.add(lat);
            if (lat < minNs) minNs = lat;
            if (lat > maxNs) maxNs = lat;
            if (startNs < firstStartNs) firstStartNs = startNs;
            if (endNs > lastEndNs) lastEndNs = endNs;
        }

        double windowSeconds() {
            if (lastEndNs <= firstStartNs) return 0.0;
            return (lastEndNs - firstStartNs) / 1_000_000_000.0;
        }

        long count() { return count.sum(); }
        double avgMs() { long c = count(); return c == 0 ? 0.0 : (sumNs.sum() / (double)c) / 1_000_000.0; }
        double minMs() { return minNs == Long.MAX_VALUE ? 0.0 : minNs / 1_000_000.0; }
        double maxMs() { return maxNs / 1_000_000.0; }
        double throughput() { double w = windowSeconds(); return w <= 0 ? 0.0 : count() / w; }

        double percentile(double p) {
            List<Long> copy;
            synchronized (latenciesNs) {
                copy = new ArrayList<>(latenciesNs);
            }
            if (copy.isEmpty()) return 0.0;
            copy.sort(Comparator.naturalOrder());
            int idx = (int)Math.ceil((p / 100.0) * copy.size()) - 1;
            idx = Math.max(0, Math.min(idx, copy.size() - 1));
            return copy.get(idx) / 1_000_000.0;
        }
    }

    private final Map<Integer, SetStats> bySet = new ConcurrentHashMap<>();

    void startSet(int setNumber) {
        bySet.computeIfAbsent(setNumber, SetStats::new);
    }

    void record(int setNumber, long startNs, long endNs) {
        bySet.computeIfAbsent(setNumber, SetStats::new).record(startNs, endNs);
    }

    void reset() { bySet.clear(); }

    void printReport() {
        if (bySet.isEmpty()) {
            System.out.println("\n" + "=".repeat(80));
            System.out.println("PERFORMANCE");
            System.out.println("=".repeat(80));
            System.out.println("No performance data recorded yet.");
            System.out.println("=".repeat(80) + "\n");
            return;
        }
        DecimalFormat f2 = new DecimalFormat("0.00");
        System.out.println("\n" + "=".repeat(80));
        System.out.println("PERFORMANCE");
        System.out.println("=".repeat(80));
        String header = String.format("%-6s %10s %10s %10s %10s %10s %10s %10s %12s",
                "Set", "Count", "Avg(ms)", "P50(ms)", "P95(ms)", "P99(ms)", "Min(ms)", "Max(ms)", "Thrpt(tps)");
        System.out.println(header);
        System.out.println("-".repeat(header.length()));
        List<Integer> sets = new ArrayList<>(bySet.keySet());
        Collections.sort(sets);
        for (int s : sets) {
            SetStats st = bySet.get(s);
            String line = String.format("%-6d %10d %10s %10s %10s %10s %10s %10s %12s",
                    s,
                    st.count(),
                    f2.format(st.avgMs()),
                    f2.format(st.percentile(50)),
                    f2.format(st.percentile(95)),
                    f2.format(st.percentile(99)),
                    f2.format(st.minMs()),
                    f2.format(st.maxMs()),
                    f2.format(st.throughput()));
            System.out.println(line);
        }
        System.out.println("=".repeat(80) + "\n");
    }
}
