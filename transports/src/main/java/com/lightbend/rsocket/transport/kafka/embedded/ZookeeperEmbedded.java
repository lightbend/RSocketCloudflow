package com.lightbend.rsocket.transport.kafka.embedded;

import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class ZookeeperEmbedded {

    private final static int defaultPort = 2181;
    private final static String ZookeeperDataFolderName = "zookeeper_data";

    private TestingServer zooKeeper = null;
    private int port = defaultPort;

    private static final Logger log = LoggerFactory.getLogger(ZookeeperEmbedded.class);

    public ZookeeperEmbedded(int port) throws Throwable {this (port, true); }
    public ZookeeperEmbedded() throws Throwable {this (defaultPort, true);}

    public ZookeeperEmbedded(int port, boolean cleanOnStart) throws Throwable {

        this.port = port;
        File zookeeperDataDir = DirectoryManagement.dataDirectory(ZookeeperDataFolderName);
        zooKeeper = new TestingServer(port, zookeeperDataDir, false);
        log.info("Zookeeper data directory is " + zookeeperDataDir);

        if (cleanOnStart) DirectoryManagement.deleteDirectory(zookeeperDataDir);

        zooKeeper.start(); // blocking operation
    }

    public void stop() {
        if (zooKeeper != null)
            try {
                zooKeeper.stop();
                zooKeeper = null;
            } catch (Throwable t) {}    // nothing to do if an exception is thrown while shutting down
    }

    public int getPort() { return port; }

    public static int getDefaultPort() { return defaultPort; }
}
