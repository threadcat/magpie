package com.threadcat.magpie;

import com.threadcat.magpie.channel.MagpieChannel;
import com.threadcat.magpie.channel.MagpieChannelDataHandler;
import com.threadcat.magpie.channel.MagpieChannelStatusListener;
import com.threadcat.magpie.internal.MagpieEndpoint;
import com.threadcat.magpie.internal.MagpieGreeting;
import com.threadcat.magpie.internal.MagpieHeartbeat;
import com.threadcat.magpie.internal.MagpiePacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Connection multiplexer.
 * Every instance assigned id and capable of both initiate connections to others and accept incoming connections.
 * i.e. this is {@link MagpieChannel} wrapper which adds message routing by id, message sequence and heartbeat control.
 * Method 'poll' supposed to be used from a single-thread dispatcher, 'send' is safe for concurrent threads.
 * <p>
 * > connection = new Magpie("service-id", port)
 * >                     .addEndpoint("host_a", port_a)
 * >                     .addEndpoint("host_b", port_b)
 * >                     .open();
 * <p>
 * > while (connection.isOpen()) {
 * >     connection.poll(dataHandler);
 * > }
 * <p>
 * > connection.send("order-gateway", dataTransformer);
 * <p>
 * Connection acceptor initiates 'greetings' for authentication.
 * Connection initiator issues heartbeats, acceptor responds with echo heartbeats.
 * Message sequence number issued and validated by both sides independently.
 * Loopback connection from the same Magpie instance is allowed (one only).
 *
 * @author threadcat
 */
public class Magpie {
    public static final short TYPE_HEARTBEAT = 0;
    public static final short TYPE_GREETING = -1;
    public static final short TYPE_DATA = 1;
    private static final Logger LOG = LoggerFactory.getLogger(Magpie.class);
    private static final String LOOPBACK = "loopback";
    private final String endpointId;
    private final MagpieChannel magpieChannel;
    private final ChannelDataHandler channelDataHandler = new ChannelDataHandler();
    private final ChannelStatusListener statusListener = new ChannelStatusListener();
    private final ConcurrentMap<String, MagpieEndpoint> connections = new ConcurrentHashMap<>();
    private final ConcurrentMap<InetSocketAddress, MagpieEndpoint> endpoints = new ConcurrentHashMap<>();
    private final List<MagpieStatusListener> listeners = new CopyOnWriteArrayList<>();

    private final MagpieGreeting greeting = new MagpieGreeting();
    private final MagpieHeartbeat heartbeatOut = new MagpieHeartbeat();
    private final Clock clock;
    private final MagpieOptions options;
    private long nextHeartbeat;


    public Magpie(String endpointId) {
        this(endpointId, 0);
    }

    public Magpie(String endpointId, int port) {
        this(endpointId, port, new MagpieOptions());
    }

    public Magpie(String endpointId, int port, MagpieOptions options) {
        this.magpieChannel = new MagpieChannel(port);
        this.magpieChannel.addStatusListener(statusListener);
        this.clock = options.getClock();
        this.endpointId = endpointId;
        this.greeting.setId(endpointId);
        this.options = options;
    }

    public Magpie addEndpoint(InetSocketAddress address) {
        magpieChannel.addEndpoint(address);
        return this;
    }

    public void removeEndpoint(InetSocketAddress address) {
        magpieChannel.removeEndpoint(address);
    }

    public Magpie addStatusListener(MagpieStatusListener listener) {
        listeners.add(listener);
        return this;
    }

    public Magpie open() {
        magpieChannel.open();
        return this;
    }

    public boolean isOpen() {
        return magpieChannel.isOpen();
    }

    public void close() {
        magpieChannel.close();
    }

    public boolean send(String endpointId, MagpieDataTransformer transformer) {
        return send(endpointId, transformer, Magpie.TYPE_DATA);
    }

    public boolean send(String endpointId, MagpieDataTransformer transformer, short dataType) {
        MagpieEndpoint endpoint = connections.get(endpointId);
        if (endpoint == null) {
            return false;
        }
        synchronized (endpoint) {
            send(endpoint.getAddress(), endpoint.incrementSequence(), endpoint.getBuffer(), transformer, dataType);
        }
        return true;
    }

    /**
     * @return false if no data received in time defined by {@link MagpieOptions#getLazyWakeup()}
     */
    public boolean poll(MagpieDataHandler dataHandler) {
        refreshHeartbeats();
        channelDataHandler.dataHandler = dataHandler;
        return magpieChannel.poll(channelDataHandler);
    }

    /**
     * Sends heartbeat through initiated connections, terminates stale connections.
     */
    private void refreshHeartbeats() {
        long currentMillis = clock.millis();
        if (currentMillis > nextHeartbeat) {
            nextHeartbeat = currentMillis + options.getHeartbeatInterval();
            for (Map.Entry<InetSocketAddress, MagpieEndpoint> entry : endpoints.entrySet()) {
                MagpieEndpoint endpoint = entry.getValue();
                final var address = endpoint.getAddress();
                if (endpoint.isHeartbeatTimeout(currentMillis)) {
                    LOG.info("Heartbeat timeout, terminating connection {} {}", endpoint.getId(), address);
                    magpieChannel.closeChannel(address);
                } else if (magpieChannel.isInitiated(address)) {
                    sendHeartbeat(endpoint);
                }
            }
        }
    }

    /**
     * Initiates greeting process - exchange of endpoint ids
     * to map address-based interactions and notifications to endpoint-id-based.
     */
    private class ChannelStatusListener implements MagpieChannelStatusListener {
        @Override
        public void statusChanged(InetSocketAddress address, MagpieStatus status) {
            switch (status) {
                case ACCEPTED:
                    MagpieEndpoint aep = new MagpieEndpoint(address, clock.millis(), options.getHeartbeatInterval(), options.getMaxMessageSize());
                    endpoints.put(address, aep);
                    sendGreeting(aep); // Initiating authentication
                    break;
                case CONNECTED:
                    MagpieEndpoint cep = new MagpieEndpoint(address, clock.millis(), options.getHeartbeatInterval(), options.getMaxMessageSize());
                    endpoints.put(address, cep);
                    break;
                case DISCONNECTED:
                    MagpieEndpoint dep = endpoints.remove(address);
                    connections.remove(dep.getId());
                    notifyStatusChanged(dep.getId(), MagpieStatus.DISCONNECTED);
                    break;
            }
        }
    }

    /**
     * Incoming data processor for greetings and heartbeats.
     * Delegates any other data type processing to next level data handler.
     */
    private class ChannelDataHandler implements MagpieChannelDataHandler {
        MagpieDataHandler dataHandler;
        MagpieEndpoint endpoint;

        @Override
        public void process(InetSocketAddress address, ByteBuffer buffer) {
            endpoint = endpoints.get(address);
            for (boolean received = true; received && MagpiePacket.lookupHeader(buffer); ) {
                received = MagpiePacket.readPacket(buffer, this::process);
            }
        }

        private void process(String none, long sequence, int type, ByteBuffer buffer, int offset, int length) {
            final var id = endpoint.getId();
            if (endpoint.verifySequence(sequence)) {
                try {
                    switch (type) {
                        case TYPE_HEARTBEAT:
                            processHeartbeat(endpoint, buffer, offset);
                            if (options.isDelegateAll()) {
                                dataHandler.process(id, sequence, type, buffer, offset, length);
                            }
                            break;
                        case TYPE_GREETING:
                            processGreeting(endpoint, buffer, offset);
                            if (options.isDelegateAll()) {
                                dataHandler.process(id, sequence, type, buffer, offset, length);
                            }
                            break;
                        default:
                            dataHandler.process(id, sequence, type, buffer, offset, length);
                    }
                } catch (Exception e) {
                    LOG.error("Failed processing type {} received from [{}] {}", type, id, endpoint.getAddress(), e);
                    notifyStatusChanged(id, MagpieStatus.ERROR);
                }
            } else {
                LOG.error("Incorrect message sequence: received " + sequence + " previous " + endpoint.lastSequence());
                notifyStatusChanged(id, MagpieStatus.ERROR);
            }
        }
    }

    private void processGreeting(MagpieEndpoint endpoint, ByteBuffer buffer, int offset) {
        greeting.fromBuffer(buffer, offset);
        String source = greeting.getId();
        final var address = endpoint.getAddress();
        boolean acceptor = magpieChannel.isAccepted(address);
        if (acceptor && source.equals(endpointId)) {
            source = LOOPBACK;
        }
        endpoint.setId(source);
        connections.put(source, endpoint);
        if (acceptor) {
            notifyStatusChanged(source, MagpieStatus.ACCEPTED);
        } else {
            sendGreeting(endpoint); // Authentication response
            notifyStatusChanged(source, MagpieStatus.CONNECTED);
        }
    }

    private void processHeartbeat(MagpieEndpoint endpoint, ByteBuffer buffer, int offset) {
        endpoint.updateLastReceived(clock.millis());
        final var address = endpoint.getAddress();
        if (magpieChannel.isAccepted(address)) {
            // Echo heartbeat from acceptor to initiator
            sendHeartbeat(endpoint);
        }
    }

    private void sendGreeting(MagpieEndpoint endpoint) {
        // Greetings are being sent and processed from a single thread as part of 'poll' invocation.
        // That synchronization protects from a sender spinning in a parallel thread on client side
        // where it is needed to send greeting response after registering incoming greeting id.
        synchronized (endpoint) {
            greeting.timestamp = clock.millis();
            greeting.setId(endpointId);
            send(endpoint.getAddress(), endpoint.incrementSequence(), endpoint.getBuffer(), greeting::toBuffer, TYPE_GREETING);
        }
    }

    private void sendHeartbeat(MagpieEndpoint endpoint) {
        synchronized (endpoint) {
            heartbeatOut.timestamp = clock.millis();
            send(endpoint.getAddress(), endpoint.incrementSequence(), endpoint.getBuffer(), heartbeatOut::toBuffer, TYPE_HEARTBEAT);
        }
    }

    private void send(InetSocketAddress address, long sequence, ByteBuffer buffer, MagpieDataTransformer transformer, short dataType) {
        buffer.clear();
        int written = MagpiePacket.writePacket(sequence, dataType, buffer, transformer);
        buffer.position(written).flip();
        magpieChannel.send(address, buffer);
    }

    private void notifyStatusChanged(String endpointId, MagpieStatus status) {
        for (MagpieStatusListener listener : listeners) {
            listener.statusChanged(endpointId, status);
        }
    }
}
