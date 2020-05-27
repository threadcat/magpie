package com.threadcat.magpie;

import java.nio.ByteBuffer;

/**
 * Payload transformer for outgoing messages.
 *
 * @author threadcat
 */
public interface MagpieDataTransformer {
    /**
     * @param buffer byte buffer to write data.
     * @param offset starting index.
     * @return length of written data.
     */
    int write(ByteBuffer buffer, int offset);
}
