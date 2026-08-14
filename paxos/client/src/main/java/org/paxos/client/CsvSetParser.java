/* I have used chatGPT to generate this file, there are few parts which I have implemeneted by myself*/

package org.paxos.client;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Parses the CSV describing transaction sets.
 */
public final class CsvSetParser {

    private static final Logger LOG = LoggerFactory.getLogger(CsvSetParser.class);

    public List<TestSet> parse(Path csvPath) throws IOException {
        Objects.requireNonNull(csvPath, "csvPath");
        if (!Files.exists(csvPath)) {
            throw new IOException("CSV file not found: " + csvPath);
        }

        Map<Integer, Builder> builders = new LinkedHashMap<>();
        Integer currentSetNumber = null;

        try (Reader reader = Files.newBufferedReader(csvPath);
             CSVReader csv = new CSVReaderBuilder(reader)
                     .withCSVParser(new CSVParserBuilder().withSeparator(',').build())
                     .build()) {

            String[] row;
            int line = 0;
            try {
                while ((row = csv.readNext()) != null) {
                    line++;
                    if (row.length == 0) {
                        continue;
                    }
                    String setToken = firstColumn(row);


                    if (setToken.startsWith("#")) {
                        continue;
                    }


                    if (!setToken.isBlank()) {
                        Integer maybeSet = parseLeadingIntOrNull(setToken);
                        if (maybeSet != null) {
                            currentSetNumber = maybeSet;
                        } else {

                            continue;
                        }
                    }


                    if (currentSetNumber == null) {
                        continue;
                    }


                    Builder builder = builders.computeIfAbsent(currentSetNumber, Builder::new);
                    int nextIndex = 1;
                    if (row.length > 1 && row[1] != null && !row[1].isBlank()) {
                        String first = row[1].trim();
                        StringBuilder sb = new StringBuilder(first);
                        if (!first.isEmpty() && first.charAt(0) == '(' && (first.charAt(first.length() - 1) != ')')) {
                            for (int k = 2; k < row.length; k++) {
                                String part = row[k] == null ? "" : row[k].trim();
                                sb.append(",").append(part);
                                if (part.endsWith(")")) {
                                    nextIndex = k;
                                    break;
                                }
                            }
                        }
                        String entry = sb.toString().trim();
                        if (!entry.isEmpty()) {
                            if (entry.equalsIgnoreCase("LF")) {
                                builder.items.add(TestItem.leaderFailure());
                            } else {
                                Integer roId = parseReadOnlyId(entry, line);
                                if (roId != null) {
                                    builder.items.add(TestItem.balance(roId));
                                } else {
                                    NodeCommand cmd = parseNodeCommand(entry);
                                    if (cmd != null) {
                                        if (cmd.fail) builder.items.add(TestItem.fail(cmd.nodeId));
                                        else builder.items.add(TestItem.recover(cmd.nodeId));
                                    } else {
                                        CsvTransaction tx = parseTransaction(entry, line);
                                        builder.transactions.add(tx);
                                        builder.items.add(TestItem.transaction(tx));
                                    }
                                }
                            }
                        }
                    }

                    int liveIdx = nextIndex + 1;
                    if (row.length > liveIdx && row[liveIdx] != null && !row[liveIdx].isBlank()) {
                        String start = row[liveIdx].trim();
                        StringBuilder ls = new StringBuilder(start);
                        if (!start.isEmpty() && start.charAt(0) == '[' && (start.charAt(start.length() - 1) != ']')) {
                            for (int k = liveIdx + 1; k < row.length; k++) {
                                String part = row[k] == null ? "" : row[k].trim();
                                ls.append(",").append(part);
                                if (part.endsWith("]")) {
                                    liveIdx = k;
                                    break;
                                }
                            }
                        }
                        builder.liveNodes = parseLiveNodes(ls.toString(), line);
                    }
                }
            } catch (CsvValidationException ex) {
                throw new IOException("Invalid CSV format at line " + (line + 1), ex);
            }
        }

        List<TestSet> sets = new ArrayList<>(builders.size());
        for (Builder builder : builders.values()) {
            sets.add(builder.build());
        }
        return sets;
    }

    private static String firstColumn(String[] row) {
        return row.length == 0 || row[0] == null ? "" : row[0].trim();
    }

    private static boolean isNumeric(String token) {
        for (int i = 0; i < token.length(); i++) {
            if (!Character.isDigit(token.charAt(i))) {
                return false;
            }
        }
        return !token.isEmpty();
    }

    private static Integer parseLeadingIntOrNull(String token) {
        if (token == null) return null;
        int n = token.length();
        int i = 0;
        while (i < n) {
            char c = token.charAt(i);
            if (Character.isWhitespace(c) || c == '\uFEFF') { i++; continue; }
            break;
        }
        int start = i;
        while (i < n && Character.isDigit(token.charAt(i))) i++;
        if (i == start) return null;
        try {
            return Integer.parseInt(token.substring(start, i));
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static CsvTransaction parseTransaction(String raw, int line) {
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("Empty transaction cell at line " + line);
        }
        if (trimmed.charAt(0) == '(' && trimmed.charAt(trimmed.length() - 1) == ')') {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        String[] parts = trimmed.split("\\s*,\\s*");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Malformed transaction '" + raw + "' at line " + line);
        }
        String senderTxt = parts[0].trim();
        String receiverTxt = parts[1].trim();
        String amountRaw = parts[2].trim();

        int senderId;
        int receiverId;
        try {
            senderId = Integer.parseInt(senderTxt);
            receiverId = Integer.parseInt(receiverTxt);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Sender/receiver must be numeric IDs at line " + line, ex);
        }
        long amount;
        try {
            amount = Long.parseLong(amountRaw);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid amount '" + amountRaw + "' at line " + line, ex);
        }
        return new CsvTransaction(Integer.toString(senderId), Integer.toString(receiverId), amount);
    }

    private static Integer parseReadOnlyId(String raw, int line) {
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        String inner = trimmed;
        if (trimmed.charAt(0) == '(' && trimmed.charAt(trimmed.length() - 1) == ')') {
            inner = trimmed.substring(1, trimmed.length() - 1).trim();
        }
        if (inner.isEmpty()) {
            return null;
        }


        if (inner.charAt(0) == '+' || inner.charAt(0) == '-') {
            return null;
        }
        for (int i = 0; i < inner.length(); i++) {
            if (!Character.isDigit(inner.charAt(i))) {
                return null;
            }
        }
        try {
            return Integer.parseInt(inner);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid balance id '" + inner + "' at line " + line, ex);
        }
    }

    private static NodeCommand parseNodeCommand(String raw) {
        String s = raw.trim();
        if (s.isEmpty()) return null;
        if (s.charAt(0) == '(' && s.charAt(s.length() - 1) == ')') {
            s = s.substring(1, s.length() - 1).trim();
        }

        String normalized = s;

        if (normalized.matches("^[FfRr].*")) {
            int lp = normalized.indexOf('(');
            int rp = normalized.lastIndexOf(')');
            if (lp > 0 && rp > lp) {
                char op = Character.toUpperCase(normalized.charAt(0));
                String inside = normalized.substring(lp + 1, rp).replaceAll("\\s+", "");
                inside = inside.toLowerCase(java.util.Locale.ROOT);
                if (inside.matches("n[1-9]")) {
                    boolean fail = (op == 'F');
                    return new NodeCommand(fail, inside);
                }
            }
        }
        return null;
    }

    private static LinkedHashSet<String> parseLiveNodes(String raw, int line) {
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("Live nodes column empty at line " + line);
        }
        if (trimmed.charAt(0) == '[' && trimmed.charAt(trimmed.length() - 1) == ']') {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        LinkedHashSet<String> nodes = new LinkedHashSet<>();
        for (String segment : trimmed.split("\\s*,\\s*")) {
            if (segment == null) {
                continue;
            }
            String token = segment.trim();
            if (!token.isEmpty()) {
                nodes.add(token);
            }
        }
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("No live nodes parsed at line " + line);
        }
        return nodes;
    }

    private static int parseInt(String value, int line) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid set number '" + value + "' at line " + line, ex);
        }
    }

    private static final class Builder {
        private final int setNumber;
        private final List<CsvTransaction> transactions = new ArrayList<>();
        private Set<String> liveNodes = Set.of();
        private final List<TestItem> items = new ArrayList<>();

        private Builder(int setNumber) {
            this.setNumber = setNumber;
        }

        private TestSet build() {
            Set<String> nodes = liveNodes;
            if (nodes.isEmpty()) {

                nodes = new LinkedHashSet<>(java.util.List.of("n1","n2","n3","n4","n5","n6","n7","n8","n9"));
            }
            return new TestSet(setNumber, List.copyOf(transactions), Set.copyOf(nodes), List.copyOf(items));
        }
    }

    public record CsvTransaction(String sender, String receiver, long amount) {
        public CsvTransaction {
            Objects.requireNonNull(sender, "sender");
            Objects.requireNonNull(receiver, "receiver");
        }
    }

    public record TestSet(int setNumber,
                          List<CsvTransaction> transactions,
                          Set<String> liveNodes,
                          List<TestItem> items) {
        public TestSet {
            Objects.requireNonNull(transactions, "transactions");
            Objects.requireNonNull(liveNodes, "liveNodes");
            Objects.requireNonNull(items, "items");
        }
    }

    public record TestItem(CsvTransaction transaction, boolean leaderFailureFlag, Integer balanceId, String failNode, String recoverNode) {
        public static TestItem transaction(CsvTransaction tx) {
            return new TestItem(Objects.requireNonNull(tx, "transaction"), false, null, null, null);
        }

        public static TestItem leaderFailure() {
            return new TestItem(null, true, null, null, null);
        }

        public static TestItem balance(int id) {
            return new TestItem(null, false, id, null, null);
        }

        public static TestItem fail(String nodeId) {
            return new TestItem(null, false, null, nodeId, null);
        }

        public static TestItem recover(String nodeId) {
            return new TestItem(null, false, null, null, nodeId);
        }

        public boolean isLeaderFailure() {
            return leaderFailureFlag;
        }

        public boolean isBalance() {
            return balanceId != null;
        }

        public boolean isFail() { return failNode != null; }
        public boolean isRecover() { return recoverNode != null; }

        public CsvTransaction transaction() {
            if (leaderFailureFlag || balanceId != null || failNode != null || recoverNode != null) {
                throw new IllegalStateException("No transaction available for this item");
            }
            return transaction;
        }
    }

    private record NodeCommand(boolean fail, String nodeId) {}
}
