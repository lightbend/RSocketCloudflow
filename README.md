# RSocket Cloudflow Ingress

This repository is the companion code to the article at [http://TODO](http://TODO)

This project is based on the articles [RSocket Intro](https://www.baeldung.com/rsocket)
[Reactive Service-to-service](https://dzone.com/articles/reactive-service-to-service-communication-with-rso-1)
and [Reactor](https://www.baeldung.com/reactor-core) and cloudflow [sensor example](https://github.com/lightbend/cloudflow/tree/master/examples/snippets/modules/ROOT/examples/sensor-data-scala)
See also presentations [here](https://www.youtube.com/watch?v=QJ3xw0MF-3U&list=PLQ4mEUUwQwBoGe4UX5mVbsNkt7DPk03Dl)

The actual RSocket code is modeled after this [one](https://github.com/b3rnoulli/rsocket-examples) and this [one](https://github.com/rsocket/rsocket-java/tree/develop/rsocket-examples/src/main/java/io/rsocket/examples/transport/tcp)


## Project structure
Project contains several modules:
* `images` - contains images for this Readme
* `interactions` - initial experimentations with RSockets, based on this [code](https://github.com/b3rnoulli/rsocket-examples).
The examples here are:
    * [Fire and forget](interactions/src/main/scala/com/lightbend/rsocket/examples/FireAndForgetClient.scala) 
    demonstrates implementation of a simple fire and forget implementation
    * [Load balanced Fire and forget](interactions/src/main/scala/com/lightbend/rsocket/examples/Multiserver.scala) 
    demonstrates implementation of a loadbalancer for fire and forget implementation
    * [Request-stream](interactions/src/main/scala/com/lightbend/rsocket/examples/StreamingClient.scala) 
    demonstrates implementation of a back preasuured request stream implementation
    * [Channel](interactions/src/main/scala/com/lightbend/rsocket/examples/ChannelEchoClient.scala) 
    demonstrates implementation of a back preasuured channel implementation
* `support` is a shared project containing Avro definitions and shared transformation code
* `sensordata` is a cloudflow implementation for the [Sensor data Processing](https://cloudflow.io/docs/current/get-started/hello-world-example.html)
* `client` is an implementation of rsocket-based data provider for publishing sensor data.

## Cloudflow implementation

The idea behind the implementation is to replace HTTP Ingress from the original implementation with the RSocket ingress.
Three different ingress implementations are provided:
* Fire and forget JSON based ingress implemented by the [class](sensordata/src/main/scala/com/lightbend/sensordata/RSocketIngress.scala).
Here Rsocket `fire and forget` interactions are used, and sensor data is passed as text JSON.
* Fire and forget Avro based ingress implemented by the [class](sensordata/src/main/scala/com/lightbend/sensordata/RSocketBinaryIngress.scala).
Here Rsocket `fire and forget` interactions are used, and sensor data is passed as Avro encoded binary.
* Stream Avro based ingress implemented by the [class](sensordata/src/main/scala/com/lightbend/sensordata/RSocketStreamIngress.scala).
Here Rsocket `request-stream` interactions are used, and sensor data is passed as Avro encoded binary. 

Any of the implementations can be used. To pick the one that you want to use, go to [blueprint](sensordata/src/main/blueprint/blueprint.conf)
and uncomment the one that you want to experiment with.

To support these three interactions there are three data publishers:
* [JSON Fire and forget](client/src/main/scala/com/lightbend/sensordata/producer/rsocket/UTF8FireAndForget.scala)
* [Binary Fire and forget](client/src/main/scala/com/lightbend/sensordata/producer/rsocket/BinaryFireAndForget.scala)
* [Binary Streaming](client/src/main/scala/com/lightbend/sensordata/producer/rsocket/BinaryRequestStream.scala)

A class [Producer runner](client/src/main/scala/com/lightbend/sensordata/producer/rsocket/ProducerRunner.scala) is provided allowing to 
pick an individual publisher.

## Running locally

To run locally:
* Select a server configuration by uncommenting your selection in `sensordata/src/main/blueprint/blueprint.conf`
* Start Cloudflow implementation
  * `sbt runLocal`
* Tail log provided by a previous command
 * `tail -f /var/log...`
* Run corresponding data provider
  * Start [Producer runner](client/src/main/scala/com/lightbend/sensordata/producer/rsocket/ProducerRunner.scala) either directly from Intellij or using the following command - `sbt "project client" run`
  * Select the option corresponding to the option selected in the blueprint

## Running on GCP
Note: This example assumes that you already have CloudFlow deployed on GKE, if you are not already familiar with 
deploying Cloudflow to GCP it is recommended to complete this 
[tutorial](https://cloudflow.io/docs/current/get-started/index.html)


To run on GCP (Assuming you have a Cloudflow instance deployed on GCP)
* Select a server configuration by uncommenting your selection in `sensordata/src/main/blueprint/blueprint.conf`
*  Edit the file `target.env`
  * ensure that your `cloudflowDockerRegistry` is correct. Probably similar to `eu.gcr.io`
  * ensure that your project id is setup for `cloudflowDockerRepository`
* Publish an image to the docker registry:
  * First make sure that you have access to the cluster `gcloud container clusters get-credentials <cluster-name>`
  * Then make sure you have access to the registry `gcloud auth configure-docker`
  * Then publish `sbt buildAndPublish`
* Deploy the application to the cluster
  * Run `kubectl-cloudflow deploy -u oauth2accesstoken eu.gcr.io/<gcloud project id>/sensor-data:2-89ce8a7 -p "$(gcloud auth print-access-token)"`
  * Check the status with `kubectl get pods -n sensor-data`
* Setup a local proxy to the ingress
  * Find the Pod name for the ingress `kubectl  get pods --all-namespaces | grep sensor-data-rsocket-ingress | awk '{ print $2 }'`
  * Create a proxy `kubectl port-forward <pod name> -n sensor-data 3000:3000`
* Run corresponding data provider
  * `sbt "project client" run`
  * Select the option corresponding to the option selected in the blueprint