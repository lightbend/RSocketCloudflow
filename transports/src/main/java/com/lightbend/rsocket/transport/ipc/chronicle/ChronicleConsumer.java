package com.lightbend.rsocket.transport.ipc.chronicle;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import reactor.core.publisher.Flux;


public class ChronicleConsumer {

    private ChronicleQueue queue;
    private ExcerptTailer tailer;

    ChronicleConsumer(String directory) {
        // Create queue
        queue = ChronicleQueue.singleBuilder(directory).build();
        // Create tailer
        tailer = queue.createTailer();
    }

    public Flux<byte[]> consumeMessages() {

        return Flux.push(sink -> {
            while (true)
                sink.next(nextMessage());
        });
    }

    private byte[] nextMessage(){
        DocumentContext dc = tailer.readingDocument();
        while(!dc.isPresent()){
            try {
                Thread.sleep(1);
            }catch (Throwable t){}
            dc = tailer.readingDocument();
        }
        Bytes bytes = dc.wire().bytes();
        int mlen = bytes.length();
        byte[] buffer = new byte[mlen];
        bytes.read(buffer);
        return buffer;
    }

    public void close(){ queue.close();}
}
