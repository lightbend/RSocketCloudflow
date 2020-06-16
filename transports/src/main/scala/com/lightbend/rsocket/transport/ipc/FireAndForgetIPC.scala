package com.lightbend.rsocket.transport.ipc

import java.io.File

import com.lightbend.rsocket.transport.ipc.RequestResponceIPC.directory
import io.rsocket._
import io.rsocket.core._
import io.rsocket.util.ByteBufPayload
import org.apache.commons.io.FileUtils
import reactor.core.publisher.Mono

object FireAndForgetIPC {

  private val directory = "tmp/boris"

  def main(args: Array[String]): Unit = {

    // Clean up
    val queueDirectory = new File(directory)
    if (queueDirectory.exists && queueDirectory.isDirectory) try FileUtils.deleteDirectory(queueDirectory)
    catch {
      case e: Throwable =>
        System.out.println("Failed to delete queue directory " + queueDirectory.getAbsolutePath + " Error: " + e)
    }

    // Create server
    val server = RSocketServer.create(SocketAcceptor.forFireAndForget((payload: Payload) => {
      // Log message
      println(s"Received 'fire-and-forget' request with payload: [${payload.getDataUtf8}]")
      Mono.empty()
    }))
      .bind(IPCServerTransport.create(directory))
      .subscribe

    // Create client
    val client = RSocketConnector.create()
      .connect(IPCClientTransport.create(directory))
      .block()

    // Send some messages
    val n = 20
    1 to n foreach { i =>
      client.fireAndForget(ByteBufPayload.create("message " + i)).block
      Thread.sleep(100);
    }
    // Wait and complete
    Thread.sleep(10000)
    client.dispose();
    server.dispose()
    System.exit(0)
  }
}