package com.lightbend.rsocket.transport.kafka.example;

import com.lightbend.rsocket.transport.kafka.embedded.KafkaEmbedded;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KafkaReactorTest {

    private static final Logger log = LoggerFactory.getLogger(SampleConsumer.class.getName());

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "demo-topic";
    private static int count = 20;

    public static void main(String[] args) throws Throwable {

        // Create Kafka
        KafkaEmbedded kafka = new KafkaEmbedded();

        // Send messages
        SampleProducer producer = new SampleProducer(BOOTSTRAP_SERVERS);
        CountDownLatch latch = new CountDownLatch(count);
        producer.sendMessages(TOPIC, count, latch);
        latch.await(10, TimeUnit.SECONDS);
        producer.close();

        // Recieve messages
        SampleConsumer consumer = new SampleConsumer(BOOTSTRAP_SERVERS);
        latch = new CountDownLatch(count);
        Disposable disposable = consumer.consumeMessages(TOPIC, latch);
        latch.await(10, TimeUnit.SECONDS);
        disposable.dispose();
        kafka.stop();
    }

}
