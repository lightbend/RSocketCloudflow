package com.lightbend.rsocket.transport.kafka.embedded;

import kafka.server.KafkaServerStartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

import kafka.server.KafkaConfig;

/**
 * Runs an in-memory, "embedded" instance of a Kafka broker, which listens at `127.0.0.1:9092` by
 * default.
 *
 */
public class KafkaEmbedded {

    private static final Logger log = LoggerFactory.getLogger(KafkaEmbedded.class);

    private final static int defaultPort = 9092;

    private final static String  KafkaDataFolderName = "kafka_data";

    private int port;
    private KafkaServerStartable broker = null;
    private ZookeeperEmbedded zookeeper = null;

    /**
     * Creates and starts an embedded Kafka broker.
     */
    public KafkaEmbedded() throws Throwable {
        this(defaultPort, ZookeeperEmbedded.getDefaultPort(), true);
    }

    public KafkaEmbedded(boolean cleanOnStart) throws Throwable {
        this(defaultPort, -1, cleanOnStart);
    }

    public KafkaEmbedded(int kafkaPort, int zookeeperServerPort, boolean cleanOnStart) throws Throwable {
        port = kafkaPort;
        File kafkaDataDir = DirectoryManagement.dataDirectory(KafkaDataFolderName);
        log.info("Kafka data directory is " + kafkaDataDir);

        if (cleanOnStart) DirectoryManagement.deleteDirectory(kafkaDataDir);
        zookeeper = new ZookeeperEmbedded(zookeeperServerPort, cleanOnStart);
        broker = KafkaServerStartable.fromProps(createKafkaProperties(port, zookeeper.getPort(), kafkaDataDir));
        broker.startup();
    }

    public void stop() {
        if (broker != null)
            broker.shutdown();
        if (zookeeper != null)
            zookeeper.stop();
    }

    /**
     * Creates a Properties instance for Kafka customized with values passed in argument.
     */
    private Properties createKafkaProperties(int kafkaPort, int zookeeperServerPort, File dataDir) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(KafkaConfig.ListenersProp(), "PLAINTEXT://localhost:" + kafkaPort);
        kafkaProperties.put(KafkaConfig.ZkConnectProp(), "localhost:" + zookeeperServerPort);
        kafkaProperties.put(KafkaConfig.ZkConnectionTimeoutMsProp(), "6000");
        kafkaProperties.put(KafkaConfig.BrokerIdProp(), "0");
        kafkaProperties.put(KafkaConfig.NumNetworkThreadsProp(), "3");
        kafkaProperties.put(KafkaConfig.NumIoThreadsProp(), "8");
        kafkaProperties.put(KafkaConfig.SocketSendBufferBytesProp(), "102400");
        kafkaProperties.put(KafkaConfig.SocketReceiveBufferBytesProp(), "102400");
        kafkaProperties.put(KafkaConfig.SocketRequestMaxBytesProp(), "104857600");
        kafkaProperties.put(KafkaConfig.NumPartitionsProp(), "1");
        kafkaProperties.put(KafkaConfig.NumRecoveryThreadsPerDataDirProp(), "1");
        kafkaProperties.put(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
        kafkaProperties.put(KafkaConfig.LogRetentionTimeHoursProp(), "2");
        kafkaProperties.put(KafkaConfig.LogSegmentBytesProp(), "1073741824");
        kafkaProperties.put(KafkaConfig.LogCleanupIntervalMsProp(), "300000");
        kafkaProperties.put(KafkaConfig.AutoCreateTopicsEnableProp(), "true");
        kafkaProperties.put(KafkaConfig.ControlledShutdownEnableProp(), "true");
        kafkaProperties.put(KafkaConfig.LogDirProp(), dataDir.getAbsolutePath());

        return kafkaProperties;
    }

}