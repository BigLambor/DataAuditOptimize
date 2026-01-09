package com.audit.counter;

import org.apache.hadoop.fs.Path;
import java.io.IOException;

/**
 * Interface for row counting implementations
 */
public interface RowCounter {
    
    /**
     * Count the number of rows in a file
     *
     * @param path HDFS file path
     * @return number of rows
     * @throws IOException if file cannot be read
     */
    long countRows(Path path) throws IOException;
}

