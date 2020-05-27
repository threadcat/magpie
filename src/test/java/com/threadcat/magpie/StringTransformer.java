package com.threadcat.magpie;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class StringTransformer {
    private String string;

    public MagpieDataTransformer writing(String string) {
        this.string = string;
        return this::toBuffer;
    }

    public int toBuffer(ByteBuffer buffer, int offset) {
        byte[] bytes = string.getBytes(StandardCharsets.US_ASCII);
        buffer.putInt(offset, bytes.length);
        buffer.put(offset + 4, bytes);
        return 4 + bytes.length;
    }

    public static String fromBuffer(ByteBuffer buffer, int offset) {
        int length = buffer.getInt(offset);
        byte[] bytes = new byte[length];
        buffer.get(offset + 4, bytes);
        return new String(bytes, StandardCharsets.US_ASCII);
    }
}
