package com.audit.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Row counter for ORC files
 * Reads row count directly from ORC file footer metadata
 */
public class OrcRowCounter implements RowCounter {
    
    private static final Logger LOG = LoggerFactory.getLogger(OrcRowCounter.class);
    
    private final Configuration conf;
    
    public OrcRowCounter(Configuration conf) {
        this.conf = conf;
    }
    
    @Override
    public long countRows(Path path) throws IOException {
        LOG.debug("Counting rows in ORC file: {}", path);
        
        try (Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf))) {
            long rowCount = reader.getNumberOfRows();
            LOG.debug("ORC file {} has {} rows", path, rowCount);
            return rowCount;
        }
    }
}

