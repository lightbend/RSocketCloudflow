package com.lightbend.rsocket.transport.ipc.chronicle;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.file.Paths;
import java.time.Duration;

public class ChronicleConsumer {

    private ChronicleQueue queue;
    private ExcerptTailer tailer;
    private String directory;
    private boolean opened = true;

    public ChronicleConsumer(String directory) {
        this.directory = directory;
        // Create queue
        queue = SingleChronicleQueueBuilder.builder(Paths.get(directory), WireType.BINARY)
                .rollCycle(RollCycles.MINUTELY)
                .build();

        // Create tailer
        tailer = queue.createTailer();
     }

    public Flux<byte[]> consumeMessages() {
        return pollAsync()
                .repeatWhenEmpty(it -> it.delayElements(Duration.ofNanos(25)))
                .repeat(() -> opened);
    }

    public Mono<byte[]> pollAsync() {
        return Mono.fromCallable(() -> nextMessage())
                .subscribeOn(Schedulers.boundedElastic());
    }

    private byte[] nextMessage(){
        try {
            DocumentContext dc = tailer.readingDocument();
            if (!dc.isPresent()) {
//                System.out.println("No data on the queue " + directory);
                return null;
            }
            Bytes bytes = dc.wire().bytes();
            int mlen = bytes.length();
            byte[] buffer = new byte[mlen];
            bytes.read(buffer);
//            System.out.println("Got data on the queue " + directory);
            return buffer;
        }catch (Throwable t){
            return null;
        }
    }

    public void close(){
        opened = false;
        queue.close();
    }
}
