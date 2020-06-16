package com.lightbend.rsocket.transport.ipc

import java.io.File

import io.rsocket._
import io.rsocket.core._
import io.rsocket.frame.decoder.PayloadDecoder
import io.rsocket.util.DefaultPayload
import org.apache.commons.io.FileUtils
import reactor.core.publisher.Mono


object RequestResponceIPC {

  private val length = 1024
  private val directory = "tmp/boris"


  def main(args: Array[String]): Unit = {

    // Clean up
    val queueDirectory = new File(directory)
    if (queueDirectory.exists && queueDirectory.isDirectory) try FileUtils.deleteDirectory(queueDirectory)
    catch {
      case e: Throwable =>
        System.out.println("Failed to delete queue directory " + queueDirectory.getAbsolutePath + " Error: " + e)
    }

    // Server
    RSocketServer.create(SocketAcceptor.forRequestResponse((payload: Payload) => {
      // Just return payload back
      Mono.just(payload)
    }))
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .bind(IPCServerTransport.create(directory))
      .subscribe

    // Client
    val socket = RSocketConnector.create()
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .connect(IPCClientTransport.create(directory))
      .block

    val n = 10000
    val data = repeatChar('x', length).getBytes()
    val start = System.currentTimeMillis()
    1 to n foreach  {_ =>
      socket.requestResponse(DefaultPayload.create(data))
        .map((payload: Payload) => {
          //          println(s"Got reply ${payload.getDataUtf8}")
          payload.release()
          payload
        })
        .block
    }
    println(s"Executed $n request/replies in ${System.currentTimeMillis() - start} ms")
    socket.dispose()
  }

  // Create a string of length
  def repeatChar(char:Char, n: Int) = List.fill(n)(char).mkString
}