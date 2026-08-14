package org.paxos.node;

import org.paxos.proto.Request;
import org.paxos.proto.Transaction;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Simple in-memory state machine for a synthetic banking workload.
 * Note: CSV persistence has been removed.
 */
public final class BankStateMachine {

    private static final long INITIAL_BALANCE = 10L;

    private final Map<String, Long> balances = new LinkedHashMap<>();

    public BankStateMachine(int nodeNumericId) {
    }

    public synchronized Result apply(Request request) {
        Objects.requireNonNull(request, "request");
        Transaction tx = request.getTrans();
        if (isNoop(tx)) {
            return Result.success(true);
        }

        String sender = tx.getSender();
        String receiver = tx.getReceiver();
        long amount = tx.getAmount();

        if (sender.isBlank() || receiver.isBlank()) {
            return Result.failure("INVALID_TX");
        }
        if (amount < 0) {
            return Result.failure("NEGATIVE_AMOUNT");
        }

        long senderBalance = balances.getOrDefault(sender, INITIAL_BALANCE);
        if (senderBalance < amount) {
            return Result.failure("INSUFFICIENT_FUNDS");
        }

        long receiverBalance = balances.getOrDefault(receiver, INITIAL_BALANCE);
        balances.put(sender, senderBalance - amount);
        balances.put(receiver, receiverBalance + amount);

        return Result.success(false);
    }

    public synchronized Map<String, Long> snapshotBalances() {
        return new LinkedHashMap<>(balances);
    }

    public synchronized String formatBalances() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, Long> entry : balances.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(entry.getKey()).append('=').append(entry.getValue());
            first = false;
        }
        return sb.toString();
    }

    private static boolean isNoop(Transaction tx) {
        return tx.getSender().isBlank() && tx.getReceiver().isBlank() && tx.getAmount() == 0;
    }

    public record Result(boolean success, String error, boolean noop) {
        private static Result success(boolean noop) {
            return new Result(true, "", noop);
        }

        private static Result failure(String error) {
            return new Result(false, error == null ? "" : error, false);
        }
    }
}