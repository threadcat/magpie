package com.threadcat.magpie.internal;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Authentication message to provide endpoint id (16 bytes).
 * Method {@link #setId(String)} supports 16-character long ASCII and UUID to transform string into two 'longs'.
 * UUID string example: 9a866451-32d2-4ab6-a35c-8ec580f94358
 *
 * @author threadcat
 */
public class MagpieGreeting {
    // UUID string example: 9a866451-32d2-4ab6-a35c-8ec580f94358
    private static final Pattern UUID_PATTERN = Pattern.compile("[0-9,a-f]{8}-[0-9,a-f]{4}-[0-9,a-f]{4}-[0-9,a-f]{4}-[0-9,a-f]{12}");
    public long timestamp;
    // Most significant bits of end point id
    public long idHigh;
    // Least significant bits of end point id
    public long idLow;

    public int toBuffer(ByteBuffer buffer, int offset) {
        buffer.putLong(offset, timestamp);
        buffer.putLong(offset + 8, idLow);
        buffer.putLong(offset + 16, idHigh);
        return 24;
    }

    public MagpieGreeting fromBuffer(ByteBuffer buffer, int offset) {
        timestamp = buffer.getLong(offset);
        idLow = buffer.getLong(offset + 8);
        idHigh = buffer.getLong(offset + 16);
        return this;
    }

    public String getId() {
        StringBuilder sb = new StringBuilder();
        if (isAscii(idLow) && isAscii(idHigh)) {
            appendAscii(sb, idLow);
            appendAscii(sb, idHigh);
        } else {
            sb.append(Long.toHexString(idHigh))
                    .append(Long.toHexString(idLow))
                    .insert(20, '-')
                    .insert(16, '-')
                    .insert(12, '-')
                    .insert(8, '-');
        }
        return sb.toString();
    }

    public void setId(String id) {
        if (UUID_PATTERN.matcher(id).matches()) {
            UUID uuid = UUID.fromString(id);
            idHigh = uuid.getMostSignificantBits();
            idLow = uuid.getLeastSignificantBits();
        } else {
            idLow = convertToLong(id, 0);
            idHigh = convertToLong(id, 8);
        }
    }

    private static boolean isAscii(long num) {
        for (int i = 0; i < 8; i++) {
            long b = num >> i * 8 & 0xffL;
            boolean ascii = (b >= ' ' && b <= '~') || b == 0L;
            if (!ascii) {
                return false;
            }
        }
        return true;
    }

    private static void appendAscii(StringBuilder sb, long n) {
        for (int i = 7; i >= 0; i--) {
            long b = n >> i * 8 & 0xffL;
            if (b != 0) {
                sb.append((char) b);
            }
        }
    }

    private static long convertToLong(String s, int offset) {
        long bits = 0L;
        for (int i = 0; i < 8 && offset + i < s.length(); i++) {
            bits <<= 8;
            bits |= (byte) s.charAt(offset + i);
        }
        return bits;
    }
}
