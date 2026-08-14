package org.paxos.node;

import org.paxos.proto.Ballot;

public final class Ballots {
    private Ballots() {}

    public static boolean greater(Ballot a, Ballot b) {
        int c = Long.compare(a.getRound(), b.getRound());
        if (c != 0) return c > 0;
        return Integer.compare(a.getLeaderId(), b.getLeaderId()) > 0;
    }

    public static boolean greaterOrEqualTo(Ballot a, Ballot b) {
        return a.getRound() > b.getRound()
                || (a.getRound() == b.getRound() && a.getLeaderId() >= b.getLeaderId());
    }
}
