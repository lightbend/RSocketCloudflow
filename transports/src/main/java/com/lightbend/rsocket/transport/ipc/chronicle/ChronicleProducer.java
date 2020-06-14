package com.lightbend.rsocket.transport.ipc.chronicle;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import org.apache.commons.io.FileUtils;
import reactor.core.publisher.Flux;

import java.io.File;

public class ChronicleProducer {

    private ChronicleQueue queue;
    private ExcerptAppender appender;
    private String directory;

    public ChronicleProducer(String directory) {

        this.directory = directory;

        // Create queue
        queue = ChronicleQueue.singleBuilder(directory).build();
        // Create appender
        appender = queue.acquireAppender();
    }

    public void sendMessages(Flux<byte[]> messages){
        messages.doOnNext(message -> {
//            System.out.println("Sending new message");
            appender.writeBytes(b -> b.write(message));
        }).subscribe();
    }

    public void close(){ queue.close();}
}
