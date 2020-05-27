package com.threadcat.magpie.channel;

public class MagpieChannelServer {
    public static void main(String[] args) {
        final var port = 11000;
        System.out.println("Starting server on port " + port);
        MagpieChannel magpie = new MagpieChannel(port)
                .addStatusListener((address, status) -> System.out.printf("%s %s\n", address, status))
                .open();
        for(;;) {
            magpie.poll((address, buffer) -> {
                System.out.printf("Received %d bytes from %s", buffer.position(), address);
                buffer.clear();
            });
        }
    }
}
