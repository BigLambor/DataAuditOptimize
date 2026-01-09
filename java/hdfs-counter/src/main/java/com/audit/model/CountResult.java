package com.audit.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Result model for row counting operation
 */
public class CountResult {
    
    private String path;
    
    @JsonProperty("row_count")
    private long rowCount;
    
    @JsonProperty("file_count")
    private int fileCount;
    
    @JsonProperty("success_file_count")
    private int successFileCount;
    
    @JsonProperty("total_size_bytes")
    private long totalSizeBytes;
    
    private String status;
    
    @JsonProperty("duration_ms")
    private long durationMs;
    
    private List<FileError> errors;
    
    // Getters and Setters
    
    public String getPath() {
        return path;
    }
    
    public void setPath(String path) {
        this.path = path;
    }
    
    public long getRowCount() {
        return rowCount;
    }
    
    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }
    
    public int getFileCount() {
        return fileCount;
    }
    
    public void setFileCount(int fileCount) {
        this.fileCount = fileCount;
    }
    
    public int getSuccessFileCount() {
        return successFileCount;
    }
    
    public void setSuccessFileCount(int successFileCount) {
        this.successFileCount = successFileCount;
    }
    
    public long getTotalSizeBytes() {
        return totalSizeBytes;
    }
    
    public void setTotalSizeBytes(long totalSizeBytes) {
        this.totalSizeBytes = totalSizeBytes;
    }
    
    public String getStatus() {
        return status;
    }
    
    public void setStatus(String status) {
        this.status = status;
    }
    
    public long getDurationMs() {
        return durationMs;
    }
    
    public void setDurationMs(long durationMs) {
        this.durationMs = durationMs;
    }
    
    public List<FileError> getErrors() {
        return errors;
    }
    
    public void setErrors(List<FileError> errors) {
        this.errors = errors;
    }
}

