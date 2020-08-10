package com.lightbend.rsocket.transport.kafka;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Closeable;
import io.rsocket.fragmentation.FragmentationDuplexConnection;
import io.rsocket.fragmentation.ReassemblyDuplexConnection;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.*;

import java.util.Objects;

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
  public Mono<Closeable> start(ConnectionAcceptor acceptor) {
    Objects.requireNonNull(acceptor, "acceptor must not be null");
    return KafkaDuplexConnection.accept(bootstrapServers, name, ByteBufAllocator.DEFAULT)
            .concatMap(acceptor)
            .then()
            .transform(Operators.<Void, Closeable>lift((__, actual) -> new KafkaCloseable(actual)));
  }

  static class KafkaCloseable extends BaseSubscriber<Void> implements Closeable {

    final MonoProcessor<Void> onClose = MonoProcessor.create();
    final CoreSubscriber<? super Closeable> actual;

    KafkaCloseable(CoreSubscriber<? super Closeable> actual) {
      this.actual = actual;
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
      super.hookOnSubscribe(subscription);
      actual.onSubscribe(this);
      actual.onNext(this);
      actual.onComplete();
    }

    @Override
    public Mono<Void> onClose() {
      return onClose;
    }

    @Override
    protected void hookOnComplete() {
      onClose.onComplete();
    }

    @Override
    protected void hookOnError(Throwable throwable) {
      onClose.onError(throwable);
    }

    @Override
    protected void hookOnCancel() {
      onClose.dispose();
    }
  }
}