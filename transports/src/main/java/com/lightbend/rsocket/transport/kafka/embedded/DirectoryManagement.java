package com.lightbend.rsocket.transport.kafka.embedded;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class DirectoryManagement {

    private static final Logger log = LoggerFactory.getLogger(DirectoryManagement.class);
    private static final String baseDir = "tmp/";

    private DirectoryManagement(){}

    public static void deleteDirectory(File directory) {
        if (directory.exists() && directory.isDirectory()) try {
            FileUtils.deleteDirectory(directory);
        } catch (Throwable e) {
            log.warn("Failed to delete directory " + directory.getAbsolutePath() + " Error: " + e);
        }
    }

    public static File dataDirectory(String directoryName) throws IllegalArgumentException {

        File dataDirectory = new File(baseDir + directoryName);
        if (dataDirectory.exists() && !dataDirectory.isDirectory())
            throw new IllegalArgumentException("Cannot use " + directoryName  + "as a directory name because a file with that name already exists in " + dataDirectory);
        return dataDirectory;
    }
}
