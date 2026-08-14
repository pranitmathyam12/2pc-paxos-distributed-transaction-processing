/* I have used chatGPT to generate this file */

package org.paxos.client;

import org.paxos.proto.ClientMeta;
import org.paxos.proto.ClientRequest;
import org.paxos.proto.Transaction;

import java.util.Objects;

/**
 * Helpers for building/parsing proto messages used by the client.
 */
public final class Messages {

    private Messages() {
    }

    public static ClientRequest buildRequest(String clientId, long timestamp, CsvSetParser.CsvTransaction tx) {
        Objects.requireNonNull(clientId, "clientId");
        Objects.requireNonNull(tx, "tx");
        return ClientRequest.newBuilder()
                .setMeta(ClientMeta.newBuilder()
                        .setClientId(clientId)
                        .setTimestamp(timestamp)
                        .build())
                .setTx(Transaction.newBuilder()
                        .setSender(tx.sender())
                        .setReceiver(tx.receiver())
                        .setAmount(tx.amount())
                        .build())
                .build();
    }
}
