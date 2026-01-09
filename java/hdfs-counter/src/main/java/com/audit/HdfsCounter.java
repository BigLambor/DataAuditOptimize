package com.audit;

import com.audit.counter.CounterFactory;
import com.audit.counter.RowCounter;
import com.audit.model.CountResult;
import com.audit.model.FileError;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * HDFS Row Counter - CLI tool to count rows in HDFS files
 * Supports ORC, Parquet, and TextFile formats
 */
public class HdfsCounter {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsCounter.class);
    
    private static final int DEFAULT_THREADS = 10;
    private static final String DEFAULT_DELIMITER = "\n";
    
    private final Configuration conf;
    private final ObjectMapper objectMapper;
    
    public HdfsCounter() {
        this(null);
    }
    
    public HdfsCounter(String hadoopConfDir) {
        this.conf = new Configuration();
        loadHadoopConfig(hadoopConfDir);
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }
    
    /**
     * Load Hadoop configuration files (core-site.xml, hdfs-site.xml)
     * Priority: 1. Specified hadoopConfDir  2. HADOOP_CONF_DIR env  3. HADOOP_HOME/etc/hadoop
     */
    private void loadHadoopConfig(String hadoopConfDir) {
        String confDir = hadoopConfDir;
        
        // Try HADOOP_CONF_DIR environment variable
        if (confDir == null || confDir.isEmpty()) {
            confDir = System.getenv("HADOOP_CONF_DIR");
        }
        
        // Try HADOOP_HOME/etc/hadoop
        if (confDir == null || confDir.isEmpty()) {
            String hadoopHome = System.getenv("HADOOP_HOME");
            if (hadoopHome != null && !hadoopHome.isEmpty()) {
                confDir = hadoopHome + "/etc/hadoop";
            }
        }
        
        if (confDir != null && !confDir.isEmpty()) {
            File confDirFile = new File(confDir);
            if (confDirFile.exists() && confDirFile.isDirectory()) {
                // Load core-site.xml
                File coreSite = new File(confDir, "core-site.xml");
                if (coreSite.exists()) {
                    conf.addResource(new Path(coreSite.getAbsolutePath()));
                    LOG.info("Loaded Hadoop config: {}", coreSite.getAbsolutePath());
                }
                
                // Load hdfs-site.xml
                File hdfsSite = new File(confDir, "hdfs-site.xml");
                if (hdfsSite.exists()) {
                    conf.addResource(new Path(hdfsSite.getAbsolutePath()));
                    LOG.info("Loaded Hadoop config: {}", hdfsSite.getAbsolutePath());
                }
            } else {
                LOG.warn("Hadoop config directory not found: {}", confDir);
            }
        } else {
            LOG.warn("No Hadoop config directory specified. Set HADOOP_CONF_DIR or HADOOP_HOME environment variable, or use --hadoop-conf option.");
        }
        
        // Initialize Kerberos authentication if enabled
        initKerberosAuth();
    }
    
    /**
     * Initialize Kerberos authentication using current user's ticket cache
     */
    private void initKerberosAuth() {
        try {
            String authType = conf.get("hadoop.security.authentication", "simple");
            if ("kerberos".equalsIgnoreCase(authType)) {
                LOG.info("Kerberos authentication enabled, initializing...");
                UserGroupInformation.setConfiguration(conf);
                
                // Check if user is already logged in (has valid ticket)
                if (UserGroupInformation.isLoginKeytabBased()) {
                    LOG.info("Using keytab-based login");
                } else {
                    // Use ticket cache (from kinit)
                    UserGroupInformation.loginUserFromSubject(null);
                    LOG.info("Using ticket cache, current user: {}", 
                            UserGroupInformation.getCurrentUser().getUserName());
                }
            }
        } catch (IOException e) {
            LOG.error("Failed to initialize Kerberos authentication: {}", e.getMessage());
        }
    }
    
    public static void main(String[] args) {
        // Pre-parse to get hadoop-conf before creating HdfsCounter
        String hadoopConfDir = null;
        for (int i = 0; i < args.length - 1; i++) {
            if ("--hadoop-conf".equals(args[i]) || "-c".equals(args[i])) {
                hadoopConfDir = args[i + 1];
                break;
            }
        }
        
        HdfsCounter counter = new HdfsCounter(hadoopConfDir);
        CountResult result = counter.run(args);
        
        try {
            String json = counter.objectMapper.writeValueAsString(result);
            System.out.println(json);
        } catch (Exception e) {
            System.err.println("{\"status\":\"failed\",\"error\":\"" + e.getMessage() + "\"}");
            System.exit(1);
        }
        
        // Exit code based on status
        if ("failed".equals(result.getStatus())) {
            System.exit(1);
        } else if ("partial".equals(result.getStatus())) {
            System.exit(2);
        }
    }
    
    public CountResult run(String[] args) {
        long startTime = System.currentTimeMillis();
        
        // Parse command line arguments
        CommandLine cmd;
        try {
            cmd = parseArgs(args);
        } catch (ParseException e) {
            return createFailedResult("", 0, "Argument parsing failed: " + e.getMessage());
        }
        
        String path = cmd.getOptionValue("path");
        String format = cmd.getOptionValue("format").toLowerCase();
        int threads = Integer.parseInt(cmd.getOptionValue("threads", String.valueOf(DEFAULT_THREADS)));
        String delimiter = cmd.getOptionValue("delimiter", DEFAULT_DELIMITER);
        
        // Handle escaped newline
        if ("\\n".equals(delimiter)) {
            delimiter = "\n";
        }
        
        LOG.info("Starting row count for path: {}, format: {}, threads: {}", path, format, threads);
        
        // Validate format
        if (!isValidFormat(format)) {
            return createFailedResult(path, System.currentTimeMillis() - startTime,
                    "Invalid format: " + format + ". Supported formats: orc, parquet, textfile");
        }
        
        // Get file system and list files
        List<FileStatus> files;
        FileSystem fs;
        try {
            Path hdfsPath = new Path(path);
            fs = hdfsPath.getFileSystem(conf);
            files = listFiles(fs, hdfsPath);
        } catch (IOException e) {
            return createFailedResult(path, System.currentTimeMillis() - startTime,
                    "Failed to access path: " + e.getMessage());
        }
        
        if (files.isEmpty()) {
            return createSuccessResult(path, 0, 0, 0, System.currentTimeMillis() - startTime);
        }
        
        // Count rows using thread pool
        return countRows(path, files, format, delimiter, threads, startTime);
    }
    
    private CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();
        
        Option pathOpt = Option.builder("p")
                .longOpt("path")
                .hasArg()
                .required()
                .desc("HDFS directory path to count")
                .build();
        
        Option formatOpt = Option.builder("f")
                .longOpt("format")
                .hasArg()
                .required()
                .desc("File format: orc, parquet, textfile")
                .build();
        
        Option threadsOpt = Option.builder("t")
                .longOpt("threads")
                .hasArg()
                .desc("Number of threads (default: " + DEFAULT_THREADS + ")")
                .build();
        
        Option delimiterOpt = Option.builder("d")
                .longOpt("delimiter")
                .hasArg()
                .desc("Line delimiter for textfile (default: \\n)")
                .build();
        
        Option hadoopConfOpt = Option.builder("c")
                .longOpt("hadoop-conf")
                .hasArg()
                .desc("Hadoop configuration directory (default: $HADOOP_CONF_DIR or $HADOOP_HOME/etc/hadoop)")
                .build();
        
        Option helpOpt = Option.builder("h")
                .longOpt("help")
                .desc("Print help message")
                .build();
        
        options.addOption(pathOpt);
        options.addOption(formatOpt);
        options.addOption(threadsOpt);
        options.addOption(delimiterOpt);
        options.addOption(hadoopConfOpt);
        options.addOption(helpOpt);
        
        CommandLineParser parser = new DefaultParser();
        
        // Check for help
        for (String arg : args) {
            if ("-h".equals(arg) || "--help".equals(arg)) {
                printHelp(options);
                System.exit(0);
            }
        }
        
        return parser.parse(options, args);
    }
    
    private void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("hdfs-counter", options);
    }
    
    private boolean isValidFormat(String format) {
        return "orc".equals(format) || "parquet".equals(format) || "textfile".equals(format);
    }
    
    /**
     * Recursively list all files under the given path
     */
    private List<FileStatus> listFiles(FileSystem fs, Path path) throws IOException {
        List<FileStatus> result = new ArrayList<>();
        
        if (!fs.exists(path)) {
            throw new IOException("Path does not exist: " + path);
        }
        
        FileStatus status = fs.getFileStatus(path);
        if (status.isFile()) {
            // Skip hidden files and success markers
            String name = path.getName();
            if (!name.startsWith("_") && !name.startsWith(".")) {
                result.add(status);
            }
        } else {
            // Directory - recursively list
            FileStatus[] children = fs.listStatus(path);
            for (FileStatus child : children) {
                String childName = child.getPath().getName();
                // Skip hidden directories (e.g., .hive-staging) and hidden files
                if (childName.startsWith("_") || childName.startsWith(".")) {
                    continue;
                }
                if (child.isDirectory()) {
                    result.addAll(listFiles(fs, child.getPath()));
                } else {
                    // File already passed the hidden/staging filter above
                    result.add(child);
                }
            }
        }
        
        return result;
    }
    
    /**
     * Count rows in files using multiple threads
     */
    private CountResult countRows(String basePath, List<FileStatus> files, String format,
                                  String delimiter, int threads, long startTime) {
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Future<FileCountResult>> futures = new ArrayList<>();
        
        RowCounter counter = CounterFactory.createCounter(format, conf, delimiter);
        
        // Submit tasks
        for (FileStatus file : files) {
            futures.add(executor.submit(() -> {
                try {
                    long count = counter.countRows(file.getPath());
                    return new FileCountResult(file.getPath().toString(), count, file.getLen(), null);
                } catch (Exception e) {
                    LOG.error("Error counting file: {}", file.getPath(), e);
                    return new FileCountResult(file.getPath().toString(), 0, file.getLen(), e.getMessage());
                }
            }));
        }
        
        // Collect results
        long totalRows = 0;
        long totalSize = 0;
        List<FileError> errors = new ArrayList<>();
        int successCount = 0;
        
        for (Future<FileCountResult> future : futures) {
            try {
                FileCountResult fcr = future.get();
                if (fcr.error == null) {
                    totalRows += fcr.rowCount;
                    totalSize += fcr.fileSize;
                    successCount++;
                } else {
                    errors.add(new FileError(fcr.filePath, fcr.error));
                }
            } catch (Exception e) {
                LOG.error("Error getting future result", e);
                errors.add(new FileError("unknown", e.getMessage()));
            }
        }
        
        // Shutdown executor and wait for termination
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        long duration = System.currentTimeMillis() - startTime;
        
        // Determine status
        String status;
        if (errors.isEmpty()) {
            status = "success";
        } else if (successCount > 0) {
            status = "partial";
        } else {
            status = "failed";
        }
        
        CountResult result = new CountResult();
        result.setPath(basePath);
        result.setRowCount(totalRows);
        result.setFileCount(files.size());
        result.setSuccessFileCount(successCount);
        result.setTotalSizeBytes(totalSize);
        result.setStatus(status);
        result.setDurationMs(duration);
        result.setErrors(errors);
        
        return result;
    }
    
    private CountResult createFailedResult(String path, long duration, String error) {
        CountResult result = new CountResult();
        result.setPath(path);
        result.setRowCount(-1);
        result.setFileCount(0);
        result.setSuccessFileCount(0);
        result.setTotalSizeBytes(0);
        result.setStatus("failed");
        result.setDurationMs(duration);
        
        List<FileError> errors = new ArrayList<>();
        errors.add(new FileError("", error));
        result.setErrors(errors);
        
        return result;
    }
    
    private CountResult createSuccessResult(String path, long rowCount, int fileCount,
                                            long totalSize, long duration) {
        CountResult result = new CountResult();
        result.setPath(path);
        result.setRowCount(rowCount);
        result.setFileCount(fileCount);
        result.setSuccessFileCount(fileCount);
        result.setTotalSizeBytes(totalSize);
        result.setStatus("success");
        result.setDurationMs(duration);
        result.setErrors(new ArrayList<>());
        return result;
    }
    
    /**
     * Internal class to hold file count result
     */
    private static class FileCountResult {
        final String filePath;
        final long rowCount;
        final long fileSize;
        final String error;
        
        FileCountResult(String filePath, long rowCount, long fileSize, String error) {
            this.filePath = filePath;
            this.rowCount = rowCount;
            this.fileSize = fileSize;
            this.error = error;
        }
    }
}

