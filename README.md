# Magpie

Multi-connection multiplexer for peer-to-peer async communications.

Single-threaded 'poll', multi-threaded 'send'.


#### Concept by example:

```java
connection = new Magpie("service-id", port)
                    .addEndpoint("host_a", port_a)
                    .addEndpoint("host_b", port_b)
                    .addEndpoint("host_c", port_c)
                    .open();
```

Polling thread:

```java
while (connection.isOpen()) {
    connection.poll(dataHandler);
}
```

Sending message (any thread):

```java
connection.send("order-gateway", dataTransformer);
```
