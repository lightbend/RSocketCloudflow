# RSocket Cloudflow Ingress

This project is based on the articles [RSocket Intro](https://www.baeldung.com/rsocket)
[Reactive Service-to-service](https://dzone.com/articles/reactive-service-to-service-communication-with-rso-1)
and [Reactor](https://www.baeldung.com/reactor-core) and cloudflow [sensor example](https://github.com/lightbend/cloudflow/tree/master/examples/snippets/modules/ROOT/examples/sensor-data-scala)

The actual RSocket code is modeled after this [one](https://github.com/b3rnoulli/rsocket-examples)

## Why RSocket?

As described [here](https://dzone.com/articles/reactive-service-to-service-communication-with-rso-1)

RSocket is a new, message-driven, binary protocol that standardizes the approach to communication in the cloud. It helps to resolve common application concerns in a consistent manner as well as it has support for multiple languages (e.g java, js, python) and transport layers (TCP, WebSocket, Aeron). 

The main RSocket characteristics are:
* Message driven - Interaction in RSocket is broken down into frames. 
* Performance - The frames are sent as a stream of bytes. It makes RSocket way more efficient than typical text-based protocols.
* Reactiveness and Flow Control - RSocket protocol fully embraces the principles stated in the Reactive Manifesto.

RSocket supports the following communication styles
![Communication](images/RSocketsInteractions.png)
* The fire and forget is designed to push the data from the sender to the receiver. 
* The request-response semantics mimics HTTP behavior.
* The request stream operation - the requester sends a single frame to the responder and gets back the stream (infinite) of data. Such interaction method enables services to switch from the pull data to the push data strategy.
* The request channel allows to stream the data from the requester to the responder and the other way around using a single physical connection. 

Here we demonstrate usage of fire and forget and and request stream implementation as an RSocket ingress.
