package com.threadcat.magpie.channel;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MagpieChannelTest {

    @Test
    void testLoopback() {
        byte[] bytes = "TestWord".getBytes(StandardCharsets.US_ASCII);
        long eightBytes = convertToLong(bytes);
        int port = 11001;
        String localhost = "localhost";
        InetSocketAddress loopbackAddress = new InetSocketAddress(localhost, port);
        MagpieChannel magpieChannel = new MagpieChannel(port)
                .addEndpoint(loopbackAddress)
                .open();
        TestHandler handler = new TestHandler();
        ByteBuffer buffer = ByteBuffer.allocate(32)
                .put(bytes)
                .flip();
        magpieChannel.send(loopbackAddress, buffer);
        // First 'poll' accepts incoming connection, the second reads data.
        for (int i = 0; i < 2; i++) {
            magpieChannel.poll(handler);
        }
        assertEquals(eightBytes, handler.eightBytes);
        magpieChannel.close();
    }

    private static class TestHandler implements MagpieChannelDataHandler {
        long eightBytes;

        @Override
        public void process(InetSocketAddress address, ByteBuffer buffer) {
            eightBytes = buffer.getLong(0);
        }
    }

    private static long convertToLong(byte[] bytes) {
        long eightBytes = 0L;
        for (int i = 0; i < 8; i++) {
            eightBytes <<= 8;
            eightBytes |= bytes[i];
        }
        return eightBytes;
    }
}