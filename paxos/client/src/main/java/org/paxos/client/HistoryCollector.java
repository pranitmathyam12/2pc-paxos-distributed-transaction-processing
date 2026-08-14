package org.paxos.client;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

final class HistoryCollector {
    private static final HistoryCollector DEFAULT = new HistoryCollector(Paths.get("client" , "history.jsonl"));

    static HistoryCollector getDefault() { return DEFAULT; }

    private final Path file;
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private HistoryCollector(Path file) {
        this.file = file;
    }

    private void ensureInit() {
        if (initialized.get()) return;
        synchronized (initialized) {
            if (initialized.get()) return;
            try {
                Files.createDirectories(file.getParent());
                if (!Files.exists(file)) {
                    Files.createFile(file);
                }
            } catch (IOException ignore) {}
            initialized.set(true);
        }
    }

    void record(String sender, String receiver, long amount, long ts) {
        Integer sId = parseIntOrNull(sender);
        Integer rId = parseIntOrNull(receiver);
        if (sId == null || rId == null) return;
        ensureInit();
        String line = String.format("{\"s\":%d,\"r\":%d,\"amt\":%d,\"ts\":%d}%n", sId, rId, amount, ts);
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.APPEND)) {
            w.write(line);
        } catch (IOException ignore) {}
    }

    private static Integer parseIntOrNull(String s) {
        if (s == null) return null;
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
