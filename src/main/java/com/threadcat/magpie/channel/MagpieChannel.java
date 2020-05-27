package com.threadcat.magpie.channel;

import com.threadcat.magpie.MagpieOptions;
import com.threadcat.magpie.MagpieStatus;
import com.threadcat.magpie.internal.MagpieException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Socket channel multiplexer. Binds listeners to local addresses and connects to specified endpoints.
 * Automatically reconnects with increasing intervals if any configured connection broken.
 * Method 'poll' supposed to be used from a single-thread dispatcher, 'send' is available to concurrent threads.
 *
 * @author threadcat
 */
public class MagpieChannel {
    private static final Logger LOG = LoggerFactory.getLogger(MagpieChannel.class);
    private final Set<InetSocketAddress> localAddresses = new CopyOnWriteArraySet<>();
    private final ConcurrentMap<InetSocketAddress, Endpoint> remoteAddresses = new ConcurrentHashMap<>();
    private final ConcurrentMap<InetSocketAddress, SocketChannel> channels = new ConcurrentHashMap<>();
    private final MagpieOptions options;
    private final List<MagpieChannelStatusListener> statusListeners = new CopyOnWriteArrayList<>();
    private Selector selector;

    /**
     * Creates client-only connection (incapable to accept incoming connections).
     */
    public MagpieChannel() {
        this(new MagpieOptions());
    }

    /**
     * Creates client-only connection with custom configuration e.g. {@link MagpieOptions#getMaxMessageSize()}.
     *
     * @param options - configuration options
     */
    public MagpieChannel(MagpieOptions options) {
        this.options = options;
    }

    /**
     * Creates connection listening on specified address.
     */
    public MagpieChannel(InetSocketAddress address) {
        this(address, new MagpieOptions());
    }

    /**
     * Creates connection listening on specified address with custom configuration
     * e.g. {@link MagpieOptions#getMaxMessageSize()}.
     *
     * @param options - configuration options
     */
    public MagpieChannel(InetSocketAddress address, MagpieOptions options) {
        this.options = options;
        localAddresses.add(address);
    }

    /**
     * Creates connection bound to all network interfaces.
     */
    public MagpieChannel(int port) {
        this(port, new MagpieOptions());
    }

    public MagpieChannel(int port, MagpieOptions options) {
        this.options = options;
        try {
            NetworkInterface.networkInterfaces()
                    .flatMap(NetworkInterface::inetAddresses)
                    .map(addr -> new InetSocketAddress(addr, port))
                    .forEach(localAddresses::add);
        } catch (SocketException e) {
            throw new MagpieException("Can not get network interfaces", e);
        }
    }

    /**
     * Binds local address to listen for incoming connections in addition to the listener assigned in constructor.
     * Extra listeners might be needed if there are multiple network interfaces and/or different ports needed.
     */
    public MagpieChannel addAcceptor(InetSocketAddress address) {
        localAddresses.add(address);
        if (isOpen()) {
            bind(address);
        }
        return this;
    }

    public boolean removeAcceptor(InetSocketAddress address) {
        boolean removed = localAddresses.remove(address);
        if (removed) {
            closeChannel(address);
        }
        return removed;
    }

    /**
     * Specified connection will be initiated on {@link MagpieChannel#open()}.
     * Logical id will be received in heartbeat message.
     */
    public MagpieChannel addEndpoint(InetSocketAddress address) {
        remoteAddresses.put(address, new Endpoint(options.getReconnectIntervals()));
        return this;
    }

    public boolean removeEndpoint(InetSocketAddress address) {
        boolean removed = remoteAddresses.remove(address) != null;
        if (removed) {
            closeChannel(address);
        }
        return removed;
    }

    public MagpieChannel addStatusListener(MagpieChannelStatusListener callback) {
        statusListeners.add(callback);
        return this;
    }

    public MagpieChannel open() throws MagpieException {
        try {
            selector = Selector.open();
        } catch (IOException e) {
            throw new MagpieException("Failed opening selector", e);
        }
        localAddresses.forEach(this::bind);
        connect();
        return this;
    }

    /**
     * Stops listener (socket acceptor) and terminates all peer connections.
     */
    public void close() {
        try {
            if (selector != null) {
                selector.close();
            }
        } catch (IOException e) {
            LOG.error("Failed closing selector", e);
        }
        channels.keySet().forEach(address -> notifyStatusChange(address, MagpieStatus.DISCONNECTED));
        channels.clear();
    }

    public void closeChannel(InetSocketAddress address) {
        SocketChannel channel = channels.remove(address);
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                //
            }
            notifyStatusChange(address, MagpieStatus.DISCONNECTED);
        }
    }

    /**
     * Sends event to specified end point.
     *
     * @return true if end point was connected and data were sent.
     */
    public boolean send(InetSocketAddress address, ByteBuffer buffer) throws MagpieException {
        SocketChannel channel = channels.get(address);
        if (channel == null) {
            return false;
        }
        synchronized (channel) {
            try {
                for (int i = 0; buffer.remaining() > 0; ) {
                    int written = channel.write(buffer);
                    switch (written) {
                        case -1:
                            LOG.error("Can not send message to disconnected endpoint {}", address);
                            closeChannel(address);
                            return false;
                        case 0:
                            if (i++ == 100) {
                                LOG.error("Sending buffer is full, disconnecting {}", address);
                                closeChannel(address);
                                return false;
                            }
                    }
                }
                return true;
            } catch (IOException e) {
                closeChannel(address);
                throw new MagpieException("Failed writing to channel " + address);
            }
        }
    }

    /**
     * Reads received messages invoking data handler for each one.
     *
     * @returns false if no data received in time defined by {@link MagpieOptions#getLazyWakeup()}.
     */
    public boolean poll(MagpieChannelDataHandler dataHandler) {
        try {
            connect();
            int n = selector.select(options.getLazyWakeup());
            if (n > 0) {
                selector.selectedKeys().removeIf(key -> processSelectionKey(key, dataHandler));
                return true;
            }
            return false;
        } catch (IOException e) {
            throw new MagpieException("Failed selecting channel keys");
        }
    }

    public boolean isOpen() {
        return selector != null && selector.isOpen();
    }

    public boolean isAccepted(InetSocketAddress address) {
        return !remoteAddresses.containsKey(address) && channels.containsKey(address);
    }

    public boolean isInitiated(InetSocketAddress address) {
        return remoteAddresses.containsKey(address) && channels.containsKey(address);
    }

    private MagpieChannel bind(InetSocketAddress inetAddress) {
        String host = inetAddress.getAddress().getHostAddress();
        int port = inetAddress.getPort();
        if (port > 0) {
            try {
                LOG.info("Starting connection acceptor {}:{}", host, port);
                ServerSocketChannel serverChannel = ServerSocketChannel.open();
                serverChannel.configureBlocking(false);
                serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, Boolean.TRUE);
                serverChannel.setOption(StandardSocketOptions.SO_REUSEPORT, Boolean.TRUE);
                serverChannel.bind(inetAddress);
                serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            } catch (IOException e) {
                throw new MagpieException(String.format("Failed initiating acceptor %s:%s", host, port), e);
            }
        }
        return this;
    }

    private boolean processSelectionKey(SelectionKey key, MagpieChannelDataHandler dataHandler) {
        if (key.isReadable()) {
            processReadableKey(key, dataHandler);
        } else if (key.isAcceptable()) {
            processAcceptableKey(key);
        }
        return true;
    }

    private void processAcceptableKey(SelectionKey key) {
        InetSocketAddress address = null;
        try {
            SocketChannel channel = ((ServerSocketChannel) key.channel()).accept();
            address = (InetSocketAddress) channel.getRemoteAddress();
            LOG.info("Accepted connection from {}", address);
            configureChannel(channel);
            channel.register(selector, SelectionKey.OP_READ, ByteBuffer.allocateDirect(options.getMaxMessageSize()));
            channels.put(address, channel);
            notifyStatusChange(address, MagpieStatus.ACCEPTED);
        } catch (IOException e) {
            LOG.error("Failed accepting request {}", address, e);
            if (address != null) {
                closeChannel(address);
            }
        }
    }

    private void processReadableKey(SelectionKey key, MagpieChannelDataHandler dataHandler) {
        InetSocketAddress address = null;
        try {
            SocketChannel channel = (SocketChannel) key.channel();
            address = (InetSocketAddress) channel.getRemoteAddress();
            ByteBuffer buffer = (ByteBuffer) key.attachment();
            int n;
            do {
                n = channel.read(buffer);
            } while (n > 0);
            if (n == 0) {
                dataHandler.process(address, buffer);
            } else {
                LOG.info("Disconnected {}", address);
                closeChannel(address);
            }
        } catch (IOException e) {
            LOG.error("Failed reading data, closing channel {}", address, e);
            if (address != null) {
                closeChannel(address);
            }
        }
    }

    private void connect() {
        long currentMillis = options.getClock().millis();
        remoteAddresses.forEach((address, endpoint) -> {
            if (!channels.containsKey(address) && endpoint.canConnect(currentMillis)) {
                try {
                    LOG.info("Connecting {}", address);
                    SocketChannel channel = SocketChannel.open(address);
                    configureChannel(channel);
                    channel.register(selector, SelectionKey.OP_READ, ByteBuffer.allocateDirect(options.getMaxMessageSize()));
                    channels.put(address, channel);
                    endpoint.reset();
                    notifyStatusChange(address, MagpieStatus.CONNECTED);
                } catch (IOException e) {
                    int pause = endpoint.updateNext(currentMillis);
                    LOG.info("Failed connecting {}, next try in {} seconds", address, pause);
                    closeChannel(address);
                }
            }
        });
    }

    private void notifyStatusChange(InetSocketAddress address, MagpieStatus status) {
        for (MagpieChannelStatusListener listener : statusListeners) {
            try {
                listener.statusChanged(address, status);
            } catch (Exception e) {
                LOG.error("Failed notifying connection status change {} {}", address, status, e);
            }
        }
    }

    private static void configureChannel(SocketChannel channel) throws IOException {
        channel.configureBlocking(false);
        channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.TRUE);
    }

    private static class Endpoint {
        private final int[] intervals;
        private long nextReconnect;
        private int counter;

        public Endpoint(int[] intervals) {
            this.intervals = intervals;
        }

        public boolean canConnect(long time) {
            return time > nextReconnect;
        }

        public int updateNext(long time) {
            int delay = intervals[counter];
            nextReconnect = time + delay * 1000;
            if (counter < intervals.length - 1) {
                counter++;
            }
            return delay;
        }

        public void reset() {
            nextReconnect = 0;
            counter = 0;
        }
    }
}
