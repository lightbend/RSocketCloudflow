package com.lightbend.rsocket.transport.kafka;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.fragmentation.FragmentationDuplexConnection;
import io.rsocket.fragmentation.ReassemblyDuplexConnection;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * An implementation of {@link ServerTransport} that connects to a {@link ClientTransport} using Kafka.
 */
public final class KafkaServerTransport implements ServerTransport<Closeable> {

  private String bootstrapServers;
  private String name;

  private KafkaServerTransport(String bootstrapServers, String name) {
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
  public static KafkaServerTransport create(String bootstrapServers, String name) {
    Objects.requireNonNull(bootstrapServers, "bootstrap servers must not be null");
    Objects.requireNonNull(name, "name must not be null");

    return new KafkaServerTransport(bootstrapServers, name);
  }

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor, int mtu) {
    Objects.requireNonNull(acceptor, "acceptor must not be null");
    Mono<Closeable> isError = FragmentationDuplexConnection.checkMtu(mtu);
    if(isError != null)
      return isError;
    MonoProcessor<Void> closeNotifier = MonoProcessor.create();
    KafkaDuplexConnection kafkaConnection =
            new KafkaDuplexConnection(bootstrapServers, name, true, ByteBufAllocator.DEFAULT, closeNotifier);
    DuplexConnection connection;
    if (mtu > 0)
      connection = new FragmentationDuplexConnection(kafkaConnection, mtu, true, "server");
    else
      connection = new ReassemblyDuplexConnection(kafkaConnection, false);

    return acceptor.apply(connection)
            .thenReturn(new Closeable() {

      @Override
      public void dispose() {
        connection.dispose();
      }

      @Override
      public Mono<Void> onClose() {
        return closeNotifier;
      }
    });
  }
}