package com.threadcat.magpie.internal;

import com.threadcat.magpie.MagpieDataHandler;
import com.threadcat.magpie.MagpieDataTransformer;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * Packet layout:
 * <p>
 * |Size (bytes)| Name
 * +------------+--------------------------------------------------------------+
 * |      6     | Marker
 * |      2     | Version
 * |      4     | Packet length including start marker and payload
 * |      4     | Message type
 * |      8     | Sequence number
 * |      N     | Payload
 *
 * @author threadcat
 */
public class MagpiePacket {
    private static final byte[] MAGPIE = "MAGPIE".getBytes(US_ASCII);
    private static final int IDX_VERSION = MAGPIE.length;
    private static final int IDX_LENGTH = IDX_VERSION + 2;
    private static final int IDX_TYPE = IDX_LENGTH + 4;
    private static final int IDX_SEQUENCE = IDX_TYPE + 4;
    private static final int IDX_DATA = IDX_SEQUENCE + 8;

    public static boolean lookupHeader(ByteBuffer buffer) {
        int n = 0;
        int position = buffer.position();
        for (int i = 0; i < position; i++) {
            if (buffer.get(i) == MAGPIE[n]) {
                n++;
                if (n == IDX_VERSION) {
                    buffer.flip().position(i - IDX_VERSION + 1).compact();
                    return true;
                }
            } else {
                n = 0;
            }
        }
        if (position == buffer.capacity()) {
            buffer.flip().position(position - n).compact();
        }
        return false;
    }

    /**
     * @return Returns total number of bytes written to byte buffer (packet length)
     */
    public static int writePacket(long sequence, int type, ByteBuffer wrBuffer, MagpieDataTransformer transformer) {
        wrBuffer.clear();
        wrBuffer.put(0, MAGPIE);
        wrBuffer.putShort(IDX_VERSION, (short) 0);
        wrBuffer.putInt(IDX_TYPE, type);
        wrBuffer.putLong(IDX_SEQUENCE, sequence);
        int length = IDX_DATA + transformer.write(wrBuffer, IDX_DATA);
        wrBuffer.putInt(IDX_LENGTH, length)
                .position(length)
                .flip();
        return length;
    }

    /**
     * @return true if buffer has more messages
     */
    public static boolean readPacket(ByteBuffer rdBuffer, MagpieDataHandler dataHandler) {
        if (rdBuffer.position() < IDX_DATA) {
            return false;
        }
        int packetLength = rdBuffer.getInt(IDX_LENGTH);
        if (packetLength > rdBuffer.capacity()) {
            rdBuffer.flip()
                    .position(IDX_VERSION)
                    .compact();
            return rdBuffer.position() > 0;
        }
        if (rdBuffer.position() >= packetLength) {
            int type = rdBuffer.getInt(IDX_TYPE);
            long sequence = rdBuffer.getLong(IDX_SEQUENCE);
            int payloadLength = packetLength - IDX_DATA;
            int position = rdBuffer.position();
            dataHandler.process(null, sequence, type, rdBuffer, IDX_DATA, payloadLength);
            rdBuffer.position(position)
                    .flip()
                    .position(packetLength)
                    .compact();
            return rdBuffer.position() >= IDX_DATA;
        } else {
            return false;
        }
    }
}