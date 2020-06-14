package com.lightbend.rsocket.transport.ipc.chronicle;

import org.apache.commons.io.FileUtils;
import reactor.core.publisher.Flux;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChronicleTest {

    private static String directory = "tmp/chronicle";
    private static String message = "Message_";
    private static int nmessages = 100;

    public static void main(String[] args) throws Throwable {

        // Clean up
        File queueDirectory = new File(directory);
        if (queueDirectory.exists() && queueDirectory.isDirectory()) try {
            FileUtils.deleteDirectory(queueDirectory);
        } catch (Throwable e) {
            System.out.println("Failed to delete queue directory " + queueDirectory.getAbsolutePath() + " Error: " + e);
        }

        // Start executor
        ExecutorService executor = Executors.newFixedThreadPool(2);

        // Create producer and consumer
        ChronicleProducer producer = new ChronicleProducer(directory);
        ChronicleConsumer consumer = new ChronicleConsumer(directory);

        // Send messages
        Runnable queueWriter = new Runnable() {
            @Override
            public void run() {
                producer.sendMessages(Flux.range(1, nmessages).map(i -> (message + i).getBytes()));
            }
        };

        // Read them back
        Runnable queueReader = new Runnable() {
            @Override
            public void run() {
                consumer.consumeMessages()
                .subscribe(message -> System.out.println("Got new message " + new String(message)));
            }
        };

        executor.submit(queueReader);
        executor.submit(queueWriter);

        Thread.sleep(1000);

        consumer.close();
        producer.close();

        System.exit(0);
    }
}