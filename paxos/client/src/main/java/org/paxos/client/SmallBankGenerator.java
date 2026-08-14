package org.paxos.client;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

final class SmallBankGenerator {
    static Result generateCsv(int setNumber,
                              int ops,
                              int readOnlyPct,
                              int crossPct,
                              double skew,
                              long seed,
                              String outFile) {
        if (ops <= 0) throw new IllegalArgumentException("ops must be > 0");
        readOnlyPct = clampPct(readOnlyPct);
        crossPct = clampPct(crossPct);
        skew = Math.max(0.0, Math.min(1.0, skew));

        ShardMapper mapper = ShardMapper.loadOrDefault();
        PairPicker picker = new PairPicker(mapper, 1, 9000, skew, seed);
        Random rng = new Random(seed ^ 0x243F6A8885A308D3L);

        int roCount = 0, rwCount = 0, crossCount = 0, intraCount = 0;
        Path path = Paths.get(outFile == null || outFile.isBlank() ? "client/test.csv" : outFile);
        try {
            if (path.getParent() != null) Files.createDirectories(path.getParent());
            try (BufferedWriter w = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
                for (int i = 0; i < ops; i++) {
                    boolean isRo = rng.nextInt(100) < readOnlyPct;
                    if (isRo) {
                        int id = picker.sampleId();
                        String line = setNumber + ",(" + id + ")";
                        w.write(line);
                        w.write('\n');
                        roCount++;
                    } else {
                        boolean cross = rng.nextInt(100) < crossPct;
                        int[] pair = cross ? picker.nextCrossPair() : picker.nextIntraPair();
                        int s = pair[0], r = pair[1];
                        int amt = 1 + rng.nextInt(9);
                        String line = setNumber + ",(" + s + "," + r + "," + amt + ")";
                        w.write(line);
                        w.write('\n');
                        rwCount++;
                        if (mapper.clusterIndexForAccountId(s) != mapper.clusterIndexForAccountId(r)) crossCount++; else intraCount++;
                    }
                }
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to write SmallBank CSV: " + path, ex);
        }

        double roPctAch = (ops == 0) ? 0 : (100.0 * roCount / ops);
        double crossPctAch = (rwCount == 0) ? 0 : (100.0 * crossCount / rwCount);
        return new Result(path.toString(), ops, roCount, rwCount, intraCount, crossCount, roPctAch, crossPctAch);
    }

    private static int clampPct(int v) { return Math.max(0, Math.min(100, v)); }

    record Result(String path,
                  int ops,
                  int readOnly,
                  int readWrite,
                  int intra,
                  int cross,
                  double readOnlyPctAchieved,
                  double crossPctAchieved) {}
}
