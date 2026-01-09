package com.audit.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Row counter for TextFile (CSV/JSON/plain text)
 * Counts occurrences of the delimiter (default: newline)
 */
public class TextFileRowCounter implements RowCounter {
    
    private static final Logger LOG = LoggerFactory.getLogger(TextFileRowCounter.class);
    
    private static final int BUFFER_SIZE = 8 * 1024 * 1024; // 8MB buffer
    
    private final Configuration conf;
    private final byte[] delimiterBytes;
    
    public TextFileRowCounter(Configuration conf, String delimiter) {
        this.conf = conf;
        this.delimiterBytes = delimiter.getBytes();
    }
    
    @Override
    public long countRows(Path path) throws IOException {
        LOG.debug("Counting rows in TextFile: {}", path);
        
        FileSystem fs = path.getFileSystem(conf);
        long fileLength = fs.getFileStatus(path).getLen();
        
        // Empty file has 0 rows
        if (fileLength == 0) {
            LOG.debug("TextFile {} is empty, 0 rows", path);
            return 0;
        }
        
        try (FSDataInputStream in = fs.open(path)) {
            long count = 0;
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            byte lastByte = 0;
            
            if (delimiterBytes.length == 1) {
                // Single byte delimiter (most common case: \n)
                byte delimByte = delimiterBytes[0];
                while ((bytesRead = in.read(buffer)) != -1) {
                    for (int i = 0; i < bytesRead; i++) {
                        if (buffer[i] == delimByte) {
                            count++;
                        }
                    }
                    lastByte = buffer[bytesRead - 1];
                }
                // If file doesn't end with delimiter, add 1 for the last line
                if (lastByte != delimByte) {
                    count++;
                }
            } else {
                // Multi-byte delimiter
                count = countMultiByteDelimiter(in, buffer, fileLength);
            }
            
            LOG.debug("TextFile {} has {} rows", path, count);
            return count;
        }
    }
    
    /**
     * Count rows with multi-byte delimiter using sliding window
     */
    private long countMultiByteDelimiter(FSDataInputStream in, byte[] buffer, long fileLength) throws IOException {
        long count = 0;
        int bytesRead;
        byte[] overlap = new byte[delimiterBytes.length - 1];
        int overlapSize = 0;
        
        // Keep a rolling window of the last delimiterBytes.length bytes across buffer boundaries
        byte[] tail = new byte[delimiterBytes.length];
        int tailSize = 0;
        
        while ((bytesRead = in.read(buffer)) != -1) {
            // Check overlap from previous buffer
            if (overlapSize > 0) {
                byte[] combined = new byte[overlapSize + Math.min(bytesRead, delimiterBytes.length - 1)];
                System.arraycopy(overlap, 0, combined, 0, overlapSize);
                System.arraycopy(buffer, 0, combined, overlapSize, combined.length - overlapSize);
                
                for (int i = 0; i <= combined.length - delimiterBytes.length; i++) {
                    if (matchesDelimiter(combined, i)) {
                        count++;
                    }
                }
            }
            
            // Search in current buffer
            for (int i = 0; i <= bytesRead - delimiterBytes.length; i++) {
                if (matchesDelimiter(buffer, i)) {
                    count++;
                }
            }
            
            // Save overlap for next iteration
            overlapSize = Math.min(delimiterBytes.length - 1, bytesRead);
            System.arraycopy(buffer, bytesRead - overlapSize, overlap, 0, overlapSize);
            
            // Update rolling tail window (last delimiterBytes.length bytes of the entire stream)
            tailSize = updateTail(tail, tailSize, buffer, bytesRead);
        }
        
        // If file doesn't end with delimiter, add 1 for the last line
        if (fileLength > 0 && !(tailSize == delimiterBytes.length && matchesDelimiter(tail, 0))) {
            count++;
        }
        
        return count;
    }
    
    /**
     * Update a fixed-size rolling tail buffer with new bytes.
     * Keeps the last {@code tail.length} bytes seen so far.
     *
     * @return updated tailSize
     */
    private int updateTail(byte[] tail, int tailSize, byte[] src, int srcLen) {
        int capacity = tail.length;
        if (srcLen >= capacity) {
            // Just keep the last 'capacity' bytes from src
            System.arraycopy(src, srcLen - capacity, tail, 0, capacity);
            return capacity;
        }
        
        // Need to append src to existing tail content (may need to drop older bytes)
        int newSize = Math.min(capacity, tailSize + srcLen);
        int bytesToDrop = Math.max(0, tailSize + srcLen - capacity);
        
        // Shift existing bytes left by bytesToDrop
        if (bytesToDrop > 0 && tailSize > 0) {
            System.arraycopy(tail, bytesToDrop, tail, 0, tailSize - bytesToDrop);
            tailSize = tailSize - bytesToDrop;
        }
        
        // Append new bytes
        System.arraycopy(src, 0, tail, tailSize, srcLen);
        return newSize;
    }
    
    private boolean matchesDelimiter(byte[] buffer, int offset) {
        for (int i = 0; i < delimiterBytes.length; i++) {
            if (buffer[offset + i] != delimiterBytes[i]) {
                return false;
            }
        }
        return true;
    }
}

