package org.paxos.client;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public record ClientConfig(Path csvPath,
                           Map<String, String> nodeTargets,
                           long timeoutMs,
                           String forcedTarget,
                           boolean auto,
                           boolean bench,
                           int benchOps,
                           int benchReadOnlyPct,
                           int benchCrossPct,
                           double benchSkew,
                           int benchSeed,
                           int benchClients) {

    private static final long DEFAULT_TIMEOUT_MS = 1500L;
    public static ClientConfig parse(String[] args) {
        Objects.requireNonNull(args, "args");
        if (args.length == 0) {
            throw new IllegalArgumentException("No arguments supplied. Expected --csv and --nodes at minimum.");
        }

        Map<String, String> options = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            String token = args[i];
            if (!token.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected argument '" + token + "'. Arguments must start with --");
            }
            String key = token.substring(2);
            if (key.isBlank()) {
                throw new IllegalArgumentException("Empty option at position " + i);
            }
            if (key.equalsIgnoreCase("help")) {
                throw new IllegalArgumentException("Usage: --csv <path> --nodes <id=host:port,...> [--timeoutMs <ms>] [--target <nodeAlias|host:port>]");
            }
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("Missing value for option --" + key);
            }
            String value = args[++i];
            options.put(key.toLowerCase(Locale.ROOT), value);
        }

        boolean bench = options.containsKey("bench") || options.containsKey("benchops");
        String csvString = options.get("csv");
        if ((csvString == null || csvString.isBlank()) && !bench) {
            throw new IllegalArgumentException("--csv is required");
        }
        if (csvString == null || csvString.isBlank()) {
            csvString = "client/bench.csv";
        }
        Path csvPath = Paths.get(csvString).toAbsolutePath().normalize();

        String nodesSpec = options.get("nodes");
        if (nodesSpec == null || nodesSpec.isBlank()) {
            throw new IllegalArgumentException("--nodes is required");
        }
        Map<String, String> nodes = parseNodes(nodesSpec);
        nodes = maybeExpandNine(nodes);
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("--nodes did not contain any entries");
        }

        long timeoutMs = DEFAULT_TIMEOUT_MS;
        if (options.containsKey("timeoutms")) {
            timeoutMs = parsePositiveLong(options.get("timeoutms"), "timeoutMs");
        }

        String target = options.get("target");
        if (target != null && target.isBlank()) {
            target = null;
        }

        boolean auto = Boolean.parseBoolean(options.getOrDefault("auto", "false"));

        int benchOps = parseNonNegativeInt(options.getOrDefault("benchops", "10000"), "benchOps");
        int benchReadOnly = parseNonNegativeInt(options.getOrDefault("benchreadonly", "50"), "benchReadOnly");
        int benchCross = parseNonNegativeInt(options.getOrDefault("benchcross", "50"), "benchCross");
        double benchSkew = Double.parseDouble(options.getOrDefault("benchskew", "0.0").trim());
        int benchSeed = parseNonNegativeInt(options.getOrDefault("benchseed", "12345"), "benchSeed");
        int benchClients = parseNonNegativeInt(options.getOrDefault("benchclients", "10"), "benchClients");

        return new ClientConfig(csvPath, Map.copyOf(nodes), timeoutMs, target, auto, bench, benchOps, benchReadOnly, benchCross, benchSkew, benchSeed, benchClients);
    }

    private static Map<String, String> parseNodes(String spec) {
        Map<String, String> mapping = new LinkedHashMap<>();
        String[] entries = spec.split(",");
        for (String entry : entries) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String[] parts = trimmed.split("=", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid node entry '" + trimmed + "'. Expected id=host:port");
            }
            String id = parts[0].trim();
            String target = parts[1].trim();
            if (id.isEmpty() || target.isEmpty()) {
                throw new IllegalArgumentException("Invalid node entry '" + trimmed + "'. Missing id or target");
            }
            if (mapping.putIfAbsent(id, target) != null) {
                throw new IllegalArgumentException("Duplicate node id '" + id + "'");
            }
        }
        return mapping;
    }

    private static Map<String, String> maybeExpandNine(Map<String, String> nodes) {
        String host = System.getenv().getOrDefault("PAXOS_NODE_HOST", "127.0.0.1").trim();
        Map<String, String> expanded = new LinkedHashMap<>(nodes);
        for (int i = 1; i <= 9; i++) {
            String alias = "n" + i;
            if (!expanded.containsKey(alias)) {
                int port = 51050 + i;
                expanded.put(alias, host + ":" + port);
            }
        }
        return expanded;
    }

    private static long parsePositiveLong(String raw, String label) {
        try {
            long value = Long.parseLong(raw.trim());
            if (value <= 0) {
                throw new IllegalArgumentException(label + " must be > 0");
            }
            return value;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid value for " + label + ": '" + raw + "'", ex);
        }
    }


    private static int parseNonNegativeInt(String raw, String label) {
        long value;
        try {
            value = Long.parseLong(raw.trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid value for " + label + ": '" + raw + "'", ex);
        }
        if (value < 0) {
            throw new IllegalArgumentException(label + " must be >= 0");
        }
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(label + " too large");
        }
        return (int) value;
    }

    public Optional<String> forcedTargetOpt() {
        return Optional.ofNullable(forcedTarget).filter(value -> !value.isBlank());
    }
}
