package com.threadcat.magpie.internal;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @author threadcat
 */
public class MagpieHeartbeat {
    public long timestamp;
    public long shutdown;

    public int toBuffer(ByteBuffer buffer, int offset) {
        buffer.putLong(offset, timestamp);
        buffer.putLong(offset + 8, shutdown);
        return 16;
    }

    public MagpieHeartbeat fromBuffer(ByteBuffer buffer, int offset) {
        timestamp = buffer.getLong(offset);
        shutdown = buffer.getLong(offset + 8);
        return this;
    }

    @Override
    public String toString() {
        return String.format("timestamp=%s shutdown=%s", isoFormat(timestamp), isoFormat(shutdown));
    }

    private static String isoFormat(long millis) {
        if (millis == 0L) {
            return "N/A";
        } else {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME);
        }
    }
}
