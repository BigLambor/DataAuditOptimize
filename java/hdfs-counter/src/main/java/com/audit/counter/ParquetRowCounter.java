package com.audit.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Row counter for Parquet files
 * Reads row count from Parquet file footer metadata
 */
public class ParquetRowCounter implements RowCounter {
    
    private static final Logger LOG = LoggerFactory.getLogger(ParquetRowCounter.class);
    
    private final Configuration conf;
    
    public ParquetRowCounter(Configuration conf) {
        this.conf = conf;
    }
    
    @Override
    public long countRows(Path path) throws IOException {
        LOG.debug("Counting rows in Parquet file: {}", path);
        
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
            // Directly read num_rows from FileMetaData footer, O(1)
            long totalRows = reader.getRecordCount();
            LOG.debug("Parquet file {} has {} rows", path, totalRows);
            return totalRows;
        }
    }
}

