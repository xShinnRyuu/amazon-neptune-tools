/*
Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.util;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncRequestBodyFromInputStreamConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.neptunedata.NeptunedataClient;
import software.amazon.awssdk.services.neptunedata.model.StartLoaderJobRequest;
import software.amazon.awssdk.services.neptunedata.model.StartLoaderJobResponse;
import software.amazon.awssdk.services.neptunedata.model.GetEngineStatusRequest;
import software.amazon.awssdk.services.neptunedata.model.GetLoaderJobStatusRequest;
import software.amazon.awssdk.services.neptunedata.model.GetLoaderJobStatusResponse;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;
import software.amazon.awssdk.transfer.s3.model.Upload;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

import com.amazonaws.services.neptune.metadata.BulkLoadConfig;

/**
 * Utility class for uploading local CSV files to Amazon S3 and loading them into Neptune
 * Designed for Neptune data loading workflows with bulk loading capability
 */
public class NeptuneBulkLoader implements AutoCloseable {
    private static final Set<String> BULK_LOAD_STATUS_CODES_COMPLETED;
    private static final Set<String> BULK_LOAD_STATUS_CODES_FAILURES;
    private static final int MAX_RETRIES = 3;
    private static final int INITIAL_BACKOFF_MS = 1000;
    private static final int MONITOR_SLEEP_TIME_MS = 1000;
    private static final int MONITOR_MAX_ATTEMPTS = 300;

    static {
        Set<String> completed = new HashSet<>();
        completed.add("LOAD_COMPLETED");
        completed.add("LOAD_COMMITTED_W_WRITE_CONFLICTS");
        BULK_LOAD_STATUS_CODES_COMPLETED = Collections.unmodifiableSet(completed);

        Set<String> failures = new HashSet<>();
        failures.add("LOAD_CANCELLED_BY_USER");
        failures.add("LOAD_CANCELLED_DUE_TO_ERRORS");
        failures.add("LOAD_UNEXPECTED_ERROR");
        failures.add("LOAD_FAILED");
        failures.add("LOAD_S3_READ_ERROR");
        failures.add("LOAD_S3_ACCESS_DENIED_ERROR");
        failures.add("LOAD_DATA_DEADLOCK");
        failures.add("LOAD_DATA_FAILED_DUE_TO_FEED_MODIFIED_OR_DELETED");
        failures.add("LOAD_FAILED_BECAUSE_DEPENDENCY_NOT_SATISFIED");
        failures.add("LOAD_FAILED_INVALID_REQUEST");
        failures.add("LOAD_CANCELLED");
        BULK_LOAD_STATUS_CODES_FAILURES = Collections.unmodifiableSet(failures);
    }

    private static final String NEPTUNE_PORT = "8182"; // Default Neptune port for HTTP API
    private static final String LOAD_ID = "loadId";
    private static final String STATUS = "status";
    private static final String OVERALL_STATUS = "overallStatus";
    private final S3TransferManager transferManager;
    private final NeptunedataClient neptuneDataClient;
    private final String bucketName;
    private final String s3Prefix;
    private final Region region;
    private final String neptuneEndpoint;
    private final String iamRoleArn;
    private final String parallelism;
    private final boolean monitor;

    public NeptuneBulkLoader(BulkLoadConfig bulkLoadConfig) {
        this.bucketName = bulkLoadConfig.getBucketName().replaceAll("/+$", "");
        this.s3Prefix = bulkLoadConfig.getS3Prefix().replaceAll("/+$", "");
        this.neptuneEndpoint = bulkLoadConfig.getNeptuneEndpoint();
        this.region = extractRegionFromEndpoint(this.neptuneEndpoint);
        this.iamRoleArn = bulkLoadConfig.getIamRoleArn();
        this.parallelism = bulkLoadConfig.getParallelism().toUpperCase();
        this.monitor = bulkLoadConfig.isMonitor();

        // Create S3AsyncClient with configuration for large file uploads
        S3AsyncClient s3AsyncClient = S3AsyncClient.builder()
                .region(region)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .multipartEnabled(true)
                .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                    .maxConcurrency(300)
                    .maxPendingConnectionAcquires(5000)
                    .connectionAcquisitionTimeout(Duration.ofHours(3))
                    .connectionTimeout(Duration.ofMinutes(5))
                    .readTimeout(Duration.ofMinutes(15))
                    .writeTimeout(Duration.ofMinutes(15))
                )
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                    .apiCallTimeout(Duration.ofMinutes(30)) // 30 minute API call timeout
                    .apiCallAttemptTimeout(Duration.ofMinutes(15)) // 15 minute per attempt
                    .build())
                .build();

        // Initialize S3 Transfer Manager with the configured S3 client
        this.transferManager = S3TransferManager.builder()
                .s3Client(s3AsyncClient)
                .build();

        // Initialize Neptune Data client
        this.neptuneDataClient = NeptunedataClient.builder()
                .region(region)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .endpointOverride(URI.create("https://" + neptuneEndpoint + ":" + NEPTUNE_PORT))
                .build();

        // Log configuration
        logConfiguration();
    }

    /**
     * Extracts the AWS region from the Neptune endpoint
     * @param endpoint The Neptune endpoint
     * @return The AWS region
     * @throws IllegalArgumentException if the endpoint format is invalid
     */
    private Region extractRegionFromEndpoint(String endpoint) {
        // Example endpoint: my-neptune-cluster.cluster[-custom]-abc123.<region>.neptune.amazonaws.com
        String[] endpointParts = endpoint.split("\\.");
        if (endpointParts.length < 4) {
            throw new IllegalArgumentException("Invalid Neptune endpoint format: " + endpoint);
        }
        return Region.of(endpointParts[2]);
    }

    /**
     * Logs the configuration for debugging purposes
     */
    private void logConfiguration() {
        System.err.println("S3 Bucket: " + this.bucketName);
        System.err.println("S3 Prefix: " + this.s3Prefix);
        System.err.println("AWS Region: " + this.region);
        System.err.println("IAM Role ARN: " + this.iamRoleArn);
        System.err.println("Neptune Endpoint: " + this.neptuneEndpoint);
        System.err.println("Bulk Load Parallelism: " + this.parallelism);
        System.err.println("Bulk Load Monitor: " + this.monitor);
        System.err.println();
    }

    /**
     * Upload Neptune vertices and edges CSV files asynchronously
     */
    public String uploadCsvFilesToS3(String filePath) throws Exception {
        System.err.println("Uploading Gremlin load data to S3...");

        // Grab the timestamp of ConvertCsv to use as S3 directory prefix
        String convertCsvTimeStamp = filePath.substring(filePath.lastIndexOf('/') + 1);

        // Check if the S3 prefix is provided, and construct the full S3 prefix using convertCsvTimeStamp
        String s3PrefixWithTimeStamp = Optional.ofNullable(s3Prefix)
            .filter(prefix  -> !prefix.isEmpty())
            .map(prefix  -> prefix + "/")
            .orElse("") + convertCsvTimeStamp;

        // Upload all files from the directory
        try {
            uploadFilesInDirectory(filePath, s3PrefixWithTimeStamp);
        } catch (Exception e) {
            System.err.println("CSV file uploads failed from directory: " + filePath);
            throw new RuntimeException("One or more CSV uploads failed.", e);
        }

        String uploadS3Uri = "s3://" + bucketName + "/" + s3PrefixWithTimeStamp+ "/";
        System.err.println("Files uploaded successfully to S3. Files available at: " + uploadS3Uri);
        return uploadS3Uri;
    }

    /**
     * Upload all files from a directory to S3 sequentially to avoid connection pool exhaustion
     */
    protected void uploadFilesInDirectory(String directoryPath, String s3Prefix) throws Exception {
        // Create a File object to check existence
        File directory = new File(directoryPath);

        if (!directory.exists() || !directory.isDirectory()) {
            throw new IllegalStateException("Directory does not exist: " + directoryPath);
        }

        System.err.println("Starting sequential upload of files from " +
            directoryPath + " to s3://" + bucketName + "/" + s3Prefix);

        // Get all files in the directory with the specified extension
        File[] csvFiles = directory.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));

        if (csvFiles == null || csvFiles.length == 0) {
            System.err.println("No files with correct extension were found in " + directoryPath);
            throw new RuntimeException("No CSV files found in directory: " + directoryPath);
        }

        // Upload files sequentially to avoid connection pool exhaustion
        uploadFilesSequentially(csvFiles, s3Prefix);
        System.err.println("Successfully uploaded all " + csvFiles.length + " files from " + directoryPath);
    }

    /**
     * Upload files sequentially (one at a time) to avoid overwhelming the connection pool
     */
    private void uploadFilesSequentially(File[] files, String s3Prefix) {
        for (int index = 0; index < files.length; index++) {
            File currentFile = files[index];
            String csvFilePath = s3Prefix + "/" + currentFile.getName();

            System.err.println("Uploading file " + (index + 1) + " of " + files.length + ": " + currentFile.getName());

            try {
                // Wait for upload to complete
                boolean success = uploadFileWithInflightCompression(currentFile.getAbsolutePath(), csvFilePath).get();

                if (!success) {
                    System.err.println("Failed to upload " + currentFile.getName() + ", stopping sequential upload");
                    throw new RuntimeException("Upload failed for file: " + currentFile.getName());
                }

                System.err.println("Successfully uploaded " + currentFile.getName() +
                    " (" + (index + 1) + "/" + files.length + ")");

            } catch (Exception e) {
                logUploadError(currentFile.getAbsolutePath(), e);
                throw new RuntimeException("Exception during upload for file: " + currentFile.getName(), e);
            }
        }

        System.err.println("Successfully uploaded all " + files.length + " files sequentially");
    }

    /**
     * Upload a single CSV file to S3 using S3TransferManager with in-flight compression
     */
    protected CompletableFuture<Boolean> uploadFileWithInflightCompression(String localFilePath, String s3Prefix) throws Exception {
        File localFile = validateLocalFile(localFilePath);

        String s3Key = s3Prefix + ".gz";
        String s3SourceUri = "s3://" + bucketName + "/" + s3Key;
        System.err.println("Starting upload with compression of " + localFilePath + " to " + s3SourceUri);
        System.err.println("File size: " + Utils.formatFileSize(localFile.length()));

        ExecutorService streamExecutor = Executors.newSingleThreadExecutor();
        PipedOutputStream pipedOut = new PipedOutputStream();
        PipedInputStream pipedIn = new PipedInputStream(pipedOut);

        try {
            CompletableFuture<Void> compressionFuture = startCompressionTask(localFile, pipedOut);
            UploadRequest uploadRequest = createUploadRequest(s3Key, pipedIn, streamExecutor);

            System.err.println("Initiating Transfer Manager upload...");
            Upload upload = transferManager.upload(uploadRequest);

            return CompletableFuture.allOf(upload.completionFuture(), compressionFuture)
                .thenApply(ignored -> handleUploadSuccess(localFile, upload))
                .exceptionally(throwable -> handleUploadFailure(localFilePath, throwable))
                .whenComplete((result, throwable) -> closeStreams(streamExecutor, pipedOut, pipedIn));
        } catch (Exception e) {
            closeStreams(streamExecutor, pipedOut, pipedIn);
            throw e;
        }
    }

    /**
     * Handle upload completion success
     * @param localFile The local file that was uploaded
     * @param upload The Upload object containing upload details
     * @return boolean indicating upload success
     */
    private boolean handleUploadSuccess(File localFile, Upload upload) {
        System.err.println("Successfully uploaded " + localFile.getName() +
            " (compressed) - ETag: " + upload.completionFuture().join().response().eTag());
        return true;
    }

    /**
     * Handle upload completion failure
     * @param localFile The local file that failed to upload
     * @param throwable The exception that occurred
     * @throws RuntimeException wrapping the original exception
     */
    private boolean handleUploadFailure(String localFilePath, Throwable throwable) {
        logUploadError(localFilePath, throwable);
        if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        } else {
            throw new RuntimeException("Upload or compression failed", throwable);
        }
    }

    /**
     * Start the compression task in a background thread
     */
    protected CompletableFuture<Void> startCompressionTask(File localFile, PipedOutputStream pipedOut) {
        return CompletableFuture.runAsync(() -> {
            try (GZIPOutputStream gzipOut = new GZIPOutputStream(pipedOut);
                 FileInputStream fis = new FileInputStream(localFile);
                 BufferedInputStream fileIn = new BufferedInputStream(fis)) {

                fileIn.transferTo(gzipOut);
                gzipOut.finish();

            } catch (IOException e) {
                throw new UncheckedIOException("Compression failed for " + localFile.getAbsolutePath(), e);
            }
        });
    }

    /**
     * Create the S3 upload request with compressed stream
     */
    private UploadRequest createUploadRequest(String s3Key, PipedInputStream pipedIn, ExecutorService streamExecutor) {
        return UploadRequest.builder()
            .putObjectRequest(putBuilder -> putBuilder
                .bucket(bucketName)
                .key(s3Key)
                .contentType("application/gzip")
                .build())
            .requestBody(AsyncRequestBody.fromInputStream(
                AsyncRequestBodyFromInputStreamConfiguration.builder()
                    .inputStream(pipedIn)
                    .executor(streamExecutor)
                    .build()
            ))
            .build();
    }

    /**
     * Log upload error details
     */
    private void logUploadError(String localFilePath, Throwable throwable) {
        System.err.println("Transfer Manager upload failed for " + localFilePath);
        System.err.println("Error type: " + throwable.getClass().getSimpleName());
        System.err.println("Error message: " + throwable.getMessage());

        if (throwable.getCause() instanceof S3Exception) {
            S3Exception s3Exception = (S3Exception) throwable.getCause();
            System.err.println("S3 error code: " + s3Exception.awsErrorDetails().errorCode());
            System.err.println("S3 error message: " + s3Exception.awsErrorDetails().errorMessage());
            System.err.println("S3 status code: " + s3Exception.statusCode());
        }
    }

    /**
     * Close piped streams and shutdown executor service
     */
    private void closeStreams(ExecutorService streamExecutor, PipedOutputStream pipedOut, PipedInputStream pipedIn) {
        streamExecutor.shutdown();
        try {
            pipedIn.close();
            pipedOut.close();
        } catch (IOException e) {
            // Log but don't fail on cleanup
            System.err.println("Warning: Failed to close piped streams: " + e.getMessage());
        }
    }

    /**
     * Start Neptune bulk load job
     */
    public String startNeptuneBulkLoad(String s3SourceUri) throws Exception {
        System.err.println("Starting Neptune bulk load...");
        if (!testNeptuneConnectivity()) {
            throw new RuntimeException("Cannot connect to Neptune endpoint: " + neptuneEndpoint);
        }

        StartLoaderJobRequest request = buildLoaderJobRequest(s3SourceUri);
        String loadId = null;

        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            try {
                loadId = executeLoaderJobRequest(request);
                return loadId;
            } catch (Exception e) {
                handleRetryLogic(attempt, e);
            }
        }
        return loadId;
    }

    private StartLoaderJobRequest buildLoaderJobRequest(String s3SourceUri) {
        return StartLoaderJobRequest.builder()
            .source(s3SourceUri)
            .format("csv")
            .s3BucketRegion(region.id())
            .iamRoleArn(iamRoleArn)
            .failOnError(false)
            .parallelism(parallelism)
            .parserConfiguration(null)
            .queueRequest(true)
            .build();
    }

    private String executeLoaderJobRequest(StartLoaderJobRequest request) {
        StartLoaderJobResponse response = neptuneDataClient.startLoaderJob(request);
        String loadId = response.payload().get(LOAD_ID);

        if (loadId == null || loadId.isEmpty()) {
            throw new RuntimeException("Failed to start Neptune bulk load - no load ID returned");
        }

        System.err.println("Neptune bulk load started successfully! Load ID: " + loadId);
        return loadId;
    }

    private void handleRetryLogic(int attempt, Exception e) throws Exception {
        if (attempt == MAX_RETRIES) {
            String errorMessage = "Failed to start Neptune bulk load after " + (MAX_RETRIES + 1) + " attempts: " + e.getMessage();
            System.err.println(errorMessage);
            throw new RuntimeException(errorMessage, e);
        }
        System.err.println("Attempt " + (attempt + 1) + " failed: " + e.getMessage());
        try {
            Thread.sleep(INITIAL_BACKOFF_MS * (1L << attempt));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Retry interrupted", ie);
        }
    }

    /**
     * Test connectivity to Neptune endpoint
     */
    protected boolean testNeptuneConnectivity() {
        try {
            System.err.println("Testing connectivity to Neptune endpoint...");

            GetEngineStatusRequest request = GetEngineStatusRequest.builder().build();
            var response = neptuneDataClient.getEngineStatus(request);

            System.err.println("Successfully connected to Neptune. Status: " +
                response.sdkHttpResponse().statusCode() + " " + response.status());
            return true;
        } catch (Exception e) {
            System.err.println("Neptune connectivity test failed: " + e.getMessage());
            return false;
        }
    }

    /**
     * Monitor Neptune bulk load progress
     */
    public void monitorLoadProgress(String loadId) throws Exception {
        System.err.println("Monitoring load progress for job: " + loadId);
        int attempt = 0;
        boolean shouldContinueMonitoring = true;

        while (attempt < MONITOR_MAX_ATTEMPTS && shouldContinueMonitoring) {
            GetLoaderJobStatusResponse response = checkNeptuneBulkLoadStatus(loadId);
            String status = extractStatusFromResponse(response);
            shouldContinueMonitoring = processMonitoringStatus(status, response);

            if (shouldContinueMonitoring) {
                Thread.sleep(MONITOR_SLEEP_TIME_MS);
                attempt++;
            }
        }

        if (attempt >= MONITOR_MAX_ATTEMPTS) {
            System.err.println("Monitoring timeouted at " +
                MONITOR_SLEEP_TIME_MS * MONITOR_MAX_ATTEMPTS + "ms. Check load status manually.");
        }
    }

    private String extractStatusFromResponse(GetLoaderJobStatusResponse response) {
        if (response.payload() != null) {
            Document payload = response.payload();
            if (payload.asMap().containsKey(OVERALL_STATUS)) {
                var overallStatus = payload.asMap().get(OVERALL_STATUS);
                if (overallStatus.asMap().containsKey(STATUS)) {
                    return overallStatus.asMap().get(STATUS).asString();
                }
            }
        } else if (response.status() != null) {
            return response.status();
        }
        return "UNKNOWN";
    }

    private boolean processMonitoringStatus(String status, GetLoaderJobStatusResponse response) {
        if (BULK_LOAD_STATUS_CODES_COMPLETED.contains(status)) {
            System.err.println("Neptune bulk load completed with status: " + status);
            return false;
        } else if (BULK_LOAD_STATUS_CODES_FAILURES.contains(status)) {
            System.err.println("Neptune bulk load failed with status: " + status);
            System.err.println("Full response: " + response.toString());
            return false;
        } else {
            System.err.println("Neptune bulk load status: " + status);
            return true;
        }
    }

    /**
     * Check the status of a Neptune bulk load job
     */
    protected GetLoaderJobStatusResponse checkNeptuneBulkLoadStatus(String loadId) throws Exception {
        GetLoaderJobStatusRequest request = GetLoaderJobStatusRequest.builder()
                .loadId(loadId)
                .build();

        GetLoaderJobStatusResponse response = neptuneDataClient.getLoaderJobStatus(request);

        if (response.sdkHttpResponse().statusCode() == 200) {
            return response;
        } else {
            throw new RuntimeException("Request failed with code " +
                response.sdkHttpResponse().statusCode() + ": " + response.toString());
        }
    }

    /**
     * Validates the local file exists
     * @param localFilePath The local file path
     * @return localFile The validated File object
     * @throws IllegalStateException if the file does not exist or is not a file
     */
    private File validateLocalFile(String localFilePath) {
        File localFile = new File(localFilePath);
        if (!localFile.exists() || !localFile.isFile()) {
            throw new IllegalStateException("File does not exist: " + localFilePath);
        }
        return localFile;
    }

    /**
     * Close the transfer manager and Neptune client, release resources (AutoCloseable implementation)
     */
    @Override
    public void close() {
        if (transferManager != null) {
            transferManager.close();
        }
        if (neptuneDataClient != null) {
            neptuneDataClient.close();
        }
    }
}
