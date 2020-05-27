package com.threadcat.magpie.channel;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * @author threadcat
 */
public interface MagpieChannelDataHandler {
    void process(InetSocketAddress address, ByteBuffer buffer);
}
