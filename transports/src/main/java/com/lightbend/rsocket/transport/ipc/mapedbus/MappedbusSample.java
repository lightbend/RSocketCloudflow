package com.lightbend.rsocket.transport.ipc.mapedbus;

import io.mappedbus.*;
import java.io.File;
import java.util.concurrent.*;

public class MappedbusSample {

    private static String file = "/tmp/test";

    public static void main(String[] args) throws Throwable {

        // Make sure that file is cleanly deleted
        File backingFile = new File(file);
        if (backingFile.exists())
            backingFile.delete();

        // Setup a reader
        MappedBusReader reader = new MappedBusReader(file, 400000L, 1000);
        reader.open();

        // Setup a writer
        MappedBusWriter writer = new MappedBusWriter(file, 400000L, 1000);
        writer.open();

        // Create writer
        Runnable mappedBusWriter = new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 300; i++) {
                    byte[] message = ("Message " + (i + 1)).getBytes();
                    try {
                        writer.write(message, 0, message.length);
//                        System.out.println("Written message, data= " + new String(message));
//                        Thread.sleep(5);
                    }catch (Throwable t){
                        System.out.println("Error writing message");
                        t.printStackTrace();
                    }
                }
            }
        };

        // Create reader
        Runnable mappedBusReader = new Runnable() {
            @Override
            public void run() {
                byte[] buffer = new byte[1000];
                while (true) {
                    try {
                        if (reader.next()) {
                            int length = reader.readBuffer(buffer, 0);
                            System.out.println("Read: length = " + length + ", data= " + new String(buffer));
                        }
                    }catch (Throwable t){
                        System.out.println("Error reading message");
                        t.printStackTrace();
                    }
                }
            }
        };

        // Run
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.execute(mappedBusWriter);
        executor.execute(mappedBusReader);
    }
}