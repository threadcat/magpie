package com.threadcat.magpie.internal;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Endpoint status data holder. Updates heartbeat time and message sequences.
 *
 * @author threadcat
 */
public class MagpieEndpoint {
    private final ByteBuffer buffer;
    private final InetSocketAddress address;
    private final long heartbeatTimeout;
    private long lastReceived;
    private long sequenceOut;
    private long sequenceIn;
    private String id;

    public MagpieEndpoint(InetSocketAddress address, long lastReceived, long heartbeatInterval, int msgSize) {
        this.buffer = ByteBuffer.allocateDirect(msgSize);
        this.address = address;
        this.heartbeatTimeout = heartbeatInterval * 3;
        this.lastReceived = lastReceived;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void updateLastReceived(long currentMillis) {
        this.lastReceived = currentMillis;
    }

    public boolean isHeartbeatTimeout(long currentMillis) {
        return currentMillis - lastReceived > heartbeatTimeout;
    }

    /**
     * @return last received sequence number.
     */
    public long lastSequence() {
        return sequenceIn;
    }

    /**
     * Verify and hold incoming message sequence.
     */
    public boolean verifySequence(long sequenceIn) {
        if (sequenceIn - this.sequenceIn == 1) {
            this.sequenceIn = sequenceIn;
            return true;
        } else {
            return false;
        }
    }

    /**
     * Increment and get outgoing message sequence.
     */
    public long incrementSequence() {
        return ++sequenceOut;
    }

    @Override
    public String toString() {
        return String.format("%s %s", id, address);
    }
}
