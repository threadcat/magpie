package com.threadcat.magpie.channel;

import java.net.InetSocketAddress;

public class MagpieChannelClient {
    public static void main(String[] args) {
        InetSocketAddress node = new InetSocketAddress("localhost", 11001);
        System.out.println("Starting client " + node);
        MagpieChannel magpie = new MagpieChannel(node)
                .addStatusListener((address, status) -> System.out.printf("%s %s\n", address, status))
                .addEndpoint(new InetSocketAddress("localhost", 11000))
                .open();
        for (; ; ) {
            magpie.poll((address, buffer) -> {
                System.out.printf("Received %d bytes from %s", buffer.position(), address);
                buffer.clear();
            });
        }
    }
}
