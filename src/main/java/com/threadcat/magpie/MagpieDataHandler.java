package com.threadcat.magpie;

import java.nio.ByteBuffer;

/**
 * Payload data handler for incoming messages.
 *
 * @author threadcat
 */
public interface MagpieDataHandler {
    void process(String source, long sequence, int type, ByteBuffer buffer, int offset, int length);
}
