package com.threadcat.magpie;

import java.time.Clock;

/**
 * Configuration options e.g. max message size etc.
 *
 * @author threadcat
 */
public class MagpieOptions {
    private int maxMessageSize = 2048; // bytes
    private int[] reconnectIntervals = {1, 3, 10, 30}; // seconds
    private long heartbeatInterval = 5000L; // milliseconds
    private long lazyWakeup = 100L; // no-data wakeup, milliseconds
    private boolean delegateAll = false; // whether to notify client data handler on greeting and heartbeats
    private Clock clock = Clock.systemUTC();

    public MagpieOptions() {
        maxMessageSize = Integer.getInteger("MAGPIE_MAX_MESSAGE_SIZE", maxMessageSize);
        heartbeatInterval = Long.getLong("MAGPIE_HEARTBEAT_INTERVAL", heartbeatInterval);
        lazyWakeup = Long.getLong("MAGPIE_LAZY_WAKEUP", lazyWakeup);
    }

    public MagpieOptions(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public int[] getReconnectIntervals() {
        return reconnectIntervals;
    }

    public void setReconnectIntervals(int... reconnectIntervals) {
        this.reconnectIntervals = reconnectIntervals;
    }

    public long getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(long heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public long getLazyWakeup() {
        return lazyWakeup;
    }

    public void setLazyWakeup(long lazyWakeup) {
        this.lazyWakeup = lazyWakeup;
    }

    public boolean isDelegateAll() {
        return delegateAll;
    }

    public void setDelegateAll(boolean delegateAll) {
        this.delegateAll = delegateAll;
    }

    public Clock getClock() {
        return clock;
    }

    public void setClock(Clock clock) {
        this.clock = clock;
    }

    @Override
    public String toString() {
        return String.format("maxMessageSize=%s heartbeatInterval=%s lazyWakeup=%s",
                maxMessageSize, heartbeatInterval, lazyWakeup);
    }
}
