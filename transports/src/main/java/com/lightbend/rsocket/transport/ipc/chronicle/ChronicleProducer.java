package com.lightbend.rsocket.transport.ipc.chronicle;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import reactor.core.publisher.Flux;

import java.nio.file.Paths;

public class ChronicleProducer {

    private ChronicleQueue queue;
    private ExcerptAppender appender;
    private String directory;

    public ChronicleProducer(String directory) {

        this.directory = directory;

        // Create queue
        queue = SingleChronicleQueueBuilder.builder(Paths.get(directory), WireType.BINARY)
                .rollCycle(RollCycles.MINUTELY)
                .build();

        // Create appender
        appender = queue.acquireAppender();
    }

    public void sendMessages(Flux<byte[]> messages){
        messages.doOnNext(message -> {
//            System.out.println("Sending new message");
            appender.writeBytes(b -> b.write(message));
//            appender.writeDocument(d -> d.getValueOut().bytes(message));
        }).subscribe();
    }

    public void close(){ queue.close();}
}
