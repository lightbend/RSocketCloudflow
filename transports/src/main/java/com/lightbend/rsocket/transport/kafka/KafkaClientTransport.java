package com.lightbend.rsocket.transport.kafka;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.fragmentation.FragmentationDuplexConnection;
import io.rsocket.fragmentation.ReassemblyDuplexConnection;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.Objects;

/**
 * An implementation of {@link ClientTransport} that connects to a {@link ServerTransport} over Kafka
 */

public class KafkaClientTransport  implements ClientTransport{

    private String bootstrapServers;
    private String name;

    private KafkaClientTransport(String bootstrapServers, String name) {
        this.bootstrapServers = bootstrapServers;
        this.name = name;
    }

    /**
     * Creates an instance.
     *
     * @param bootstrapServers the bootstrap servers string
     * @param name the name of the topic used for this connection
     * @return a new instance
     * @throws NullPointerException if {@code name} is {@code null}
     */
    public static KafkaClientTransport create(String bootstrapServers, String name) {
        Objects.requireNonNull(bootstrapServers, "bootstrap servers must not be null");
        Objects.requireNonNull(name, "name must not be null");

        return new KafkaClientTransport(bootstrapServers, name);
    }

    @Override
    public Mono<DuplexConnection> connect() {
        return KafkaDuplexConnection.create(bootstrapServers, name, ByteBufAllocator.DEFAULT);
    }
}