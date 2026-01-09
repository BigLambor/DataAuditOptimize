package com.audit.counter;

import org.apache.hadoop.conf.Configuration;

/**
 * Factory for creating row counter instances
 */
public class CounterFactory {
    
    /**
     * Create a row counter for the specified format
     *
     * @param format file format (orc, parquet, textfile)
     * @param conf Hadoop configuration
     * @param delimiter line delimiter for textfile
     * @return appropriate RowCounter implementation
     */
    public static RowCounter createCounter(String format, Configuration conf, String delimiter) {
        switch (format.toLowerCase()) {
            case "orc":
                return new OrcRowCounter(conf);
            case "parquet":
                return new ParquetRowCounter(conf);
            case "textfile":
                return new TextFileRowCounter(conf, delimiter);
            default:
                throw new IllegalArgumentException("Unsupported format: " + format);
        }
    }
}

