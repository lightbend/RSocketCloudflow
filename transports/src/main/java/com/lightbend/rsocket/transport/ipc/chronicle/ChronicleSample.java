package com.lightbend.rsocket.transport.ipc.chronicle;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChronicleSample {

    private static String directory = "tmp/chronicle";
    private static int nmessages = 10000;
    private static int nchar = 4096;
    private static byte[] message = buildString(nchar, 'x');

    public static void main(String[] args) throws Throwable {

        // Clean up
        File queueDirectory = new File(directory);
        if (queueDirectory.exists() && queueDirectory.isDirectory())try {
            FileUtils.deleteDirectory(queueDirectory);
        } catch (Throwable e) {
            System.out.println("Failed to delete directory " + queueDirectory.getAbsolutePath() + " Error: " + e);
        }

        // Create queue
        ChronicleQueue queue = ChronicleQueue.singleBuilder(directory).build();
        // Writer
        ExcerptAppender appender = queue.acquireAppender();
        // reader
        ExcerptTailer tailer = queue.createTailer();

        // Create writer
        Runnable queueWriter = new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < nmessages; i++) {
                     appender.writeBytes(b -> b.write(message));
                }
            }
        };

        // Create reader
        Runnable queueReader = new Runnable() {
            @Override
            public void run() {
                int messages = 0;
                while (messages < nmessages) {
                    DocumentContext dc = tailer.readingDocument();
                    if (dc.isPresent()) {
                        Bytes bytes = dc.wire().bytes();
                        int mlen = bytes.length();
                        byte[] buffer = new byte[mlen];
                        bytes.read(buffer);
                        messages++;
//                        System.out.println("Got message " + new String(buffer));
                    }
                    try {
                        Thread.sleep(1);
                    }catch (Throwable t) {}
                }
            }
        };

        // Run
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CompletableFuture[] futures = new CompletableFuture[2];
        long start = System.currentTimeMillis();
        futures[0] = CompletableFuture.runAsync(queueWriter, executor);
        futures[1] = CompletableFuture.runAsync(queueReader, executor);
        CompletableFuture.allOf(futures).join();
        System.out.println("Done " + nmessages + " request/replies in " + (System.currentTimeMillis() -  start) + "ms");
    }

    private static byte[] buildString(int len, char character){
        StringBuffer outputBuffer = new StringBuffer(len);
        for (int i = 0; i < len; i++){
            outputBuffer.append(character);
        }
        return outputBuffer.toString().getBytes();
    }
}