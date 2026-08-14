package org.paxos.node;

import org.paxos.proto.Ballot;
import org.paxos.proto.Request;
import org.paxos.proto.TwoPcStage;

import java.util.Objects;

public final class LogEntry {
    private final Request request;
    private volatile Ballot acceptedBallot;
    private volatile NodeState.Phase phase;
    private final TwoPcStage twoPcStage;

    public LogEntry(Request request, Ballot acceptedBallot, NodeState.Phase phase) {
        this(request, acceptedBallot, phase, TwoPcStage.TWO_PC_NONE);
    }

    public LogEntry(Request request, Ballot acceptedBallot, NodeState.Phase phase, TwoPcStage twoPcStage) {
        this.request = Objects.requireNonNull(request, "request");
        this.acceptedBallot = acceptedBallot;
        this.phase = Objects.requireNonNull(phase, "phase");
        this.twoPcStage = twoPcStage == null ? TwoPcStage.TWO_PC_NONE : twoPcStage;
    }

    public Request getRequest() {
        return request;
    }

    public Ballot getAcceptedBallot() {
        return acceptedBallot;
    }

    public NodeState.Phase getPhase() {
        return phase;
    }

    public void setAcceptedBallot(Ballot ballot) {
        this.acceptedBallot = ballot;
    }

    public synchronized void advancePhase(NodeState.Phase newPhase) {
        if (newPhase.ordinal() > this.phase.ordinal()) {
            this.phase = newPhase;
        }
    }

    public TwoPcStage getTwoPcStage() {
        return twoPcStage;
    }

    @Override
    public String toString() {
        return String.format("LogEntry{phase=%s, stage=%s, ballot=%s, request=%s}",
                phase,
                twoPcStage,
                acceptedBallot != null ? acceptedBallot.getRound() : "null",
                request);
    }
}