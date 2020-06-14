package com.lightbend.rsocket.transport.ipc;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.fragmentation.FragmentationDuplexConnection;
import io.rsocket.fragmentation.ReassemblyDuplexConnection;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import org.apache.commons.io.FileUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.io.File;
import java.util.Objects;

/**
 * An implementation of {@link ServerTransport} that connects to a {@link ClientTransport} over IPC.
 */
public final class IPCServerTransport implements ServerTransport<Closeable> {

  private String directory;

  private IPCServerTransport(String directory) {

      // Clean up directory
      File queueDirectory = new File(directory);
      if (queueDirectory.exists() && queueDirectory.isDirectory()) try {
          FileUtils.deleteDirectory(queueDirectory);
      } catch (Throwable e) {
          System.out.println("Failed to delete queue directory " + queueDirectory.getAbsolutePath() + " Error: " + e);
      }

      this.directory = directory;
  }

  /**
   * Creates an instance.
   *
   * @param directory the name of the directory used for this connection
   * @return a new instance
   * @throws NullPointerException if {@code name} is {@code null}
   */
  public static IPCServerTransport create(String directory) {
    Objects.requireNonNull(directory, "name must not be null");

    return new IPCServerTransport(directory);
  }

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor, int mtu) {
    Objects.requireNonNull(acceptor, "acceptor must not be null");
    Mono<Closeable> isError = FragmentationDuplexConnection.checkMtu(mtu);
    if(isError != null)
      return isError;
    MonoProcessor<Void> closeNotifier = MonoProcessor.create();
    IPCDuplexConnection IPCConnection =
            new IPCDuplexConnection( directory, true, ByteBufAllocator.DEFAULT, closeNotifier);
    DuplexConnection connection;
    if (mtu > 0)
      connection = new FragmentationDuplexConnection(IPCConnection, mtu, true, "server");
    else
      connection = new ReassemblyDuplexConnection(IPCConnection, false);

    return acceptor.apply(connection)
            .thenReturn(new Closeable() {
     @Override
      public void dispose() {
        connection.dispose();
        closeNotifier.onComplete();
     }

     @Override
     public boolean isDisposed() { return closeNotifier.isDisposed(); }

     @Override
     public Mono<Void> onClose() { return closeNotifier; }
    });
  }
}