package com.lightbend.rsocket.transport.ipc;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.fragmentation.FragmentationDuplexConnection;
import io.rsocket.fragmentation.ReassemblyDuplexConnection;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.Objects;

/**
 * An implementation of {@link ClientTransport} that connects to a {@link ServerTransport} over IPC
 */

public class IPCClientTransport implements ClientTransport{

    private String directory;

    private IPCClientTransport(String directory) {
        this.directory = directory;
    }

    /**
     * Creates an instance.
     *
     * @param directory the name of the directory used for this connection
     * @return a new instance
     * @throws NullPointerException if {@code name} is {@code null}
     */
    public static IPCClientTransport create(String directory) {
        Objects.requireNonNull(directory, "directory must not be null");

        return new IPCClientTransport(directory);
    }

    @Override
    public Mono<DuplexConnection> connect(int mtu) {
        Mono<DuplexConnection> isError = FragmentationDuplexConnection.checkMtu(mtu);
        if(isError != null)
            return isError;
        MonoProcessor<Void> closeNotifier = MonoProcessor.create();
        IPCDuplexConnection connection =
        new IPCDuplexConnection(directory, false, ByteBufAllocator.DEFAULT, closeNotifier);

        if (mtu > 0) {
            return Mono.just(
                    new FragmentationDuplexConnection(connection, mtu, true, "client"));
        } else {
            return Mono.just( new ReassemblyDuplexConnection(connection, false));
        }
    }
}
