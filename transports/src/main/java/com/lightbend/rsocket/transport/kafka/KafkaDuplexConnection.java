package com.lightbend.rsocket.transport.kafka;

import io.netty.buffer.ByteBuf;
import static io.netty.buffer.Unpooled.*;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.CharsetUtil;
import io.rsocket.DuplexConnection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.*;

/** An implementation of {@link DuplexConnection} that connects using Kafka. */
final class KafkaDuplexConnection implements DuplexConnection {

  private KafkaConsumer consumer;                     // Consumer
  private KafkaProducer producer;                     // Producer

  private final ByteBufAllocator allocator;
  private final MonoProcessor<Void> onClose = MonoProcessor.create();

  static Mono<DuplexConnection> create(String bootstrapServers, String name, ByteBufAllocator allocator) {
    final String topicName = UUID.randomUUID().toString();
    return new KafkaProducer(bootstrapServers, name, "client")
            .send(Mono.fromSupplier(() -> {
              final ByteBuf initFrame = allocator.buffer();
              initFrame.writeCharSequence(topicName, CharsetUtil.UTF_8);
              return initFrame;
            }))
            .then(
                    new KafkaConsumer(bootstrapServers, name + "-reply", "client").getKafkaFlux()
                    .filter(bb -> {
                      final String topicNameConfirmation = bb.toString(CharsetUtil.UTF_8);
                      bb.release();
                      return topicNameConfirmation.equals(topicName);
                    })
                    .next()
                    .then(Mono.fromSupplier(() -> new KafkaDuplexConnection(bootstrapServers, topicName, false, allocator)))
            );
  }


    static Flux<KafkaDuplexConnection> accept(String bootstrapServers, String name, ByteBufAllocator allocator) {
        final KafkaProducer producer = new KafkaProducer(bootstrapServers, name + "-reply", "server");
        return new KafkaConsumer(bootstrapServers, name, "server").getKafkaFlux()
                        .concatMap(bb -> {
                            final String topicName = bb.toString(CharsetUtil.UTF_8);
                            return producer.send(Mono.just(bb))
                                    .then(Mono.fromSupplier(() -> new KafkaDuplexConnection(bootstrapServers, topicName, true, allocator)));
                        });
    }

   KafkaDuplexConnection(String bootstrapServers, String name, boolean server, ByteBufAllocator allocator){
    this.allocator = Objects.requireNonNull(allocator, "allocator must not be null");

    if (server) {
      producer = new KafkaProducer(bootstrapServers, name + "-reply", "server");
      consumer = new KafkaConsumer(bootstrapServers, name, "server");
    }
    else{
      producer = new KafkaProducer(bootstrapServers, name, "client");
      consumer = new KafkaConsumer(bootstrapServers, name + "-reply", "client");
    }
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    return producer.send(frames)
            .then();
  }

  @Override
  public Flux<ByteBuf> receive() {
    return consumer.getKafkaFlux();
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
    onClose.onComplete();
  }

  private static class KafkaProducer{

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class.getName());
    private final KafkaSender<byte[], byte[]> sender;
    private final String topic;

    public KafkaProducer(String bootstrapServers, String topic, String prefix){

      this.topic = topic;
      Map<String, Object> props = new HashMap<>();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      props.put(ProducerConfig.CLIENT_ID_CONFIG, prefix + topic + "-producer");
      props.put(ProducerConfig.ACKS_CONFIG, "all");
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
      SenderOptions<byte[], byte[]> senderOptions = SenderOptions.<byte[], byte[]>create(props).maxInFlight(128);

      sender = KafkaSender.create(senderOptions);
    }

    public Mono<Void> send(Publisher<ByteBuf> frames){
      return sender.<Integer>send(Flux.from(frames)
              .map(frame -> {
                byte[] bytes = new byte[frame.readableBytes()];
                frame.readBytes(bytes);
//                System.out.println("Sending new message to topic " + topic);
                frame.release();
                return SenderRecord.create(new ProducerRecord<>(topic,null,bytes), 1);
              }))
              .then();
    }

    public void close() {
      sender.close();
    }
  }

  private static class KafkaConsumer{

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class.getName());
    private final ReceiverOptions<byte[], byte[]> receiverOptions;
    private Flux<ReceiverRecord<byte[], byte[]>> kafkaFlux;

    public KafkaConsumer(String bootstrapServers, String topic, String prefix){

      Map<String, Object> props = new HashMap<>();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString() + "-consumer");
      props.put(ConsumerConfig.GROUP_ID_CONFIG, prefix + "-" + topic + "-group");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

      receiverOptions = ReceiverOptions.create(props);

      ReceiverOptions<byte[], byte[]> options = receiverOptions.subscription(Collections.singleton(topic))
              .commitInterval(Duration.ZERO)
              .addAssignListener(partitions -> log.debug("onPartitionsAssigned {}", partitions))
              .addRevokeListener(partitions -> log.debug("onPartitionsRevoked {}", partitions));
      kafkaFlux = KafkaReceiver.create(options).receive();
    }

    public Flux<ByteBuf> getKafkaFlux() {
      return kafkaFlux.map(receiverRecord -> {
//        System.out.println("Recieving new message to topic " + receiverRecord.topic());
        return copiedBuffer(receiverRecord.value());
      });
    }
  }
}