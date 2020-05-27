package com.threadcat.magpie;

import com.threadcat.latency.common.LinuxTaskSet;

import java.nio.ByteBuffer;

/**
 * Echo server mirroring {@link MagpieEchoClient} requests.
 */
public class MagpieEchoServer {
    static final String THREAD_NAME = "echo-server";
    static final String MAGPIE_ID = "echo-server";

    public static void main(String[] args) throws Exception {
        Thread.currentThread().setName(THREAD_NAME);
        LinuxTaskSet.setCpuMask(THREAD_NAME, "0x4");
        EchoTransformer transformer = new EchoTransformer();
        Magpie magpie = new Magpie(MAGPIE_ID, 11001).open();
        System.out.println("Started");
        for (; ; ) {
            magpie.poll(((source, sequence, type, buffer, offset, length) -> {
                if (type == Magpie.TYPE_DATA) {
                    transformer.bufferA = buffer;
                    transformer.offset = offset;
                    transformer.length = length;
                    magpie.send(source, transformer::toBuffer);
                }
            }));
        }
    }

    static class EchoTransformer {
        ByteBuffer bufferA;
        int offset;
        int length;

        public int toBuffer(ByteBuffer bufferB, int offsetB) {
            int position = bufferA.position();
            int limit = bufferA.limit();
            bufferB.position(offsetB)
                    .put(bufferA.position(offset)
                            .limit(offset + length));
            bufferA.position(position)
                    .limit(limit);
            return length;
        }
    }
}
