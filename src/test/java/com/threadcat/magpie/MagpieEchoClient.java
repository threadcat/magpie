package com.threadcat.magpie;

import com.threadcat.latency.common.LinuxTaskSet;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Testing round-trip latency with {@link MagpieEchoServer}:
 * Executed 1000000 times, latency 19.749 microseconds
 *
 * One-way latency measured by 'watch-service-latency' utility (same hardware, OS and JVM):
 * Executed 100000 pings in 1.783 seconds, one-way max latency 277.731 µs, average 8.873 µs
 *
 * Conclusion: timing cost of Magpie functionality is ( 19.224 - 8.873 * 2 = 1.478 ) microseconds, ~ 8%
 */
public class MagpieEchoClient {
    static final String THREAD_NAME = "echo-client";
    static final String MAGPIE_ID = "echo-client";

    public static void main(String[] args) throws Exception {
        Thread.currentThread().setName(THREAD_NAME);
        LinuxTaskSet.setCpuMask(THREAD_NAME, "0x8");
        InetSocketAddress echoAddress = new InetSocketAddress("localhost", 11001);
        Magpie magpie = new Magpie(MAGPIE_ID, 11002);
        int warmup = 300_000;
        int measure = 100_000;
        EchoClient client = new EchoClient(magpie, warmup, measure);
        magpie.addEndpoint(echoAddress)
                .addStatusListener(client::onConnect)
                .open();
        System.out.println("Started");
        for (; client.counter != client.total; ) {
            magpie.poll(client::onReceive);
        }
        System.out.printf("Executed %d times, latency %.3f microseconds\n",
                client.counter, (double) (client.stop - client.start) / 1000 / measure);
    }

    static class EchoClient {
        static final byte[] TEST_BYTES = "Test String".getBytes(StandardCharsets.US_ASCII);
        final Magpie magpie;
        final int warmpup;
        final int total;
        int counter;
        long start;
        long stop;

        public EchoClient(Magpie magpie, int warmpup, int measure) {
            this.magpie = magpie;
            this.warmpup = warmpup;
            this.total = warmpup + measure;
        }

        public void onConnect(String endpointId, MagpieStatus status) {
            if (status == MagpieStatus.CONNECTED) {
                start = System.nanoTime();
                magpie.send("echo-server", this::toBuffer);
            }
        }

        public void onReceive(String source, long sequence, int type, ByteBuffer buffer, int offset, int length) {
            if (type == Magpie.TYPE_DATA) {
                stop = System.nanoTime();
                counter++;
                if (counter == warmpup) {
                    System.out.println("Warming up finished");
                    start = System.nanoTime();
                }
                if (counter < total) {
                    magpie.send("echo-server", this::toBuffer);
                }
            }
        }

        private int toBuffer(ByteBuffer buffer, int offset) {
            buffer.put(offset, TEST_BYTES);
            return TEST_BYTES.length;
        }
    }
}
