package com.lightbend.rsocket.transport.kafka;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.fragmentation.FragmentationDuplexConnection;
import io.rsocket.fragmentation.ReassemblyDuplexConnection;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
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

  private Mono<Closeable> connect(ConnectionAcceptor acceptor, int mtu) {
    return Mono.defer(
            () ->  {
              ServerDuplexConnectionAcceptor server = new ServerDuplexConnectionAcceptor(name, acceptor, mtu);
              MonoProcessor<Void> closeNotifier = MonoProcessor.create();
              KafkaDuplexConnection connection =
                      new KafkaDuplexConnection(bootstrapServers, name, true, ByteBufAllocator.DEFAULT, closeNotifier);
              server.accept(connection);
              return Mono.just(server);
            });
  }

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor, int mtu) {
    Objects.requireNonNull(acceptor, "acceptor must not be null");
    Mono<Closeable> isError = FragmentationDuplexConnection.checkMtu(mtu);
    return isError != null ? isError : connect(acceptor, mtu);
  }

/*
  @Override
  public Mono<CloseableChannel> start(ConnectionAcceptor acceptor, int mtu) {
    Objects.requireNonNull(acceptor, "acceptor must not be null");
    Mono<CloseableChannel> isError = FragmentationDuplexConnection.checkMtu(mtu);
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
    acceptor.apply(connection).subscribe();

  } */

    /**
     * A {@link Consumer} of {@link DuplexConnection} that is called when a server has been created.
     */
  static class ServerDuplexConnectionAcceptor implements Consumer<DuplexConnection>, Closeable {

    private final ConnectionAcceptor acceptor;

    private final MonoProcessor<Void> onClose = MonoProcessor.create();

    private final int mtu;

    /**
     * Creates a new instance
     *
     * @param name the name of the server
     * @param acceptor the {@link ConnectionAcceptor} to call when the server has been created
     * @throws NullPointerException if {@code name} or {@code acceptor} is {@code null}
     */
    ServerDuplexConnectionAcceptor(String name, ConnectionAcceptor acceptor, int mtu) {
      Objects.requireNonNull(name, "name must not be null");

      this.acceptor = Objects.requireNonNull(acceptor, "acceptor must not be null");
      this.mtu = mtu;
    }

    @Override
    public void accept(DuplexConnection duplexConnection) {
      Objects.requireNonNull(duplexConnection, "duplexConnection must not be null");

      if (mtu > 0) {
        duplexConnection =
                new FragmentationDuplexConnection(duplexConnection, mtu, false, "server");
      } else {
        duplexConnection = new ReassemblyDuplexConnection(duplexConnection, false);
      }

      acceptor.apply(duplexConnection).subscribe();
    }

    @Override
    public void dispose() {
      onClose.onComplete();
    }

    @Override
    public boolean isDisposed() {
      return onClose.isDisposed();
    }

    @Override
    public Mono<Void> onClose() {
      return onClose;
    }
  }
}