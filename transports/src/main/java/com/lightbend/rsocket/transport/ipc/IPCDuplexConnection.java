package com.lightbend.rsocket.transport.ipc;

import com.lightbend.rsocket.transport.ipc.chronicle.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.*;

import static io.netty.buffer.Unpooled.copiedBuffer;

/** An implementation of {@link DuplexConnection} that connects using Chronicle Queue. */
final class IPCDuplexConnection implements DuplexConnection {

  private String directory;                               // Name of the input directory
  private ChronicleConsumer consumer;                     // Consumer
  private ChronicleProducer producer;                     // Producer

  private final ByteBufAllocator allocator;
  private final MonoProcessor<Void> onClose;

  public IPCDuplexConnection(String directory, boolean server, ByteBufAllocator allocator, MonoProcessor<Void> onClose){
    this.allocator = Objects.requireNonNull(allocator, "allocator must not be null");
    this.onClose = Objects.requireNonNull(onClose, "onClose must not be null");
    this.directory = Objects.requireNonNull(directory, "directory must not be null");

    if(server){
      producer = new ChronicleProducer(directory + "/reply");
      consumer = new ChronicleConsumer( directory );
    }
    else{
      producer = new ChronicleProducer(directory );
      consumer = new ChronicleConsumer( directory + "/reply");
    }
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    producer.sendMessages(Flux.from(frames)
            .map(frame -> {
              byte[] bytes = new byte[frame.readableBytes()];
              frame.readBytes(bytes);
              return bytes;
            }));
    return Mono.empty();
  }

  @Override
  public Flux<ByteBuf> receive() {
    return consumer.consumeMessages().map(bytes -> copiedBuffer(bytes));
  }

  @Override
  public ByteBufAllocator alloc() {
    return allocator;
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  @Override
  public void dispose() {
    producer.close();
    consumer.close();
    onClose.onComplete();
  }
}