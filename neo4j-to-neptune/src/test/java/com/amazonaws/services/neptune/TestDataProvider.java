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

package com.amazonaws.services.neptune;

import java.io.File;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.amazonaws.services.neptune.metadata.BulkLoadConfig;
import com.amazonaws.services.neptune.metadata.ConversionConfig;
import com.amazonaws.services.neptune.metadata.EdgeMetadata;
import com.amazonaws.services.neptune.metadata.MultiValuedNodePropertyPolicy;
import com.amazonaws.services.neptune.metadata.MultiValuedRelationshipPropertyPolicy;
import com.amazonaws.services.neptune.metadata.PropertyValueParser;
import com.amazonaws.services.neptune.metadata.VertexMetadata;
import com.amazonaws.services.neptune.util.CSVUtils;
import com.amazonaws.services.neptune.util.NeptuneBulkLoader;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

/**
 * Test data provider utility class for Neptune bulk loader tests
 * Contains helper methods for creating mock data and accessing private fields
 */
public class TestDataProvider {

    // Test constants
    public static final String BUCKET = "test-neptune-bucket";
    public static final String S3_PREFIX = "test-prefix";
    public static final String S3_DEFAULT = "";
    public static final String CONVERT_CSV_TIMESTAMP = "1751659751530";
    public static final String S3_SOURCE_URI = "s3://" + BUCKET + "/" + S3_PREFIX + CONVERT_CSV_TIMESTAMP + "/";
    public static final Region REGION_US_EAST_2 = Region.US_EAST_2;
    public static final String NEPTUNE_ENDPOINT = "test-neptune.cluster-abc123." + REGION_US_EAST_2 + ".neptune.amazonaws.com";
    public static final String IAM_ROLE_ARN = "arn:aws:iam::123456789012:role/TestNeptuneRole";
    public static final String TEMP_FOLDER_NAME = "TEST_TEMP_FOLDER";
    public static final String VERTICIES_CSV = "vertices.csv";
    public static final String EDGES_CSV = "edges.csv";
    public static final String S3_KEY_FOR_UPLOAD_FILE_ASYNC_VERTICES = S3_PREFIX + "/" + VERTICIES_CSV;
    public static final String S3_KEY_FOR_UPLOAD_FILE_ASYNC_EDGES = S3_PREFIX + "/" + EDGES_CSV;
    public static final String LOAD_ID_0 = "00000000-0000-0000-0000-000000000000";
    public static final String LOAD_ID_1 = "00000000-0000-0000-0000-000000000001";
    public static final String BULK_LOAD_PARALLELISM_LOW = "LOW";
    public static final String BULK_LOAD_PARALLELISM_MEDIUM = "MEDIUM";
    public static final String BULK_LOAD_PARALLELISM_HIGH = "HIGH";
    public static final String BULK_LOAD_PARALLELISM_OVERSUBSCRIBE = "OVERSUBSCRIBE";
    public static final Boolean BOOLEAN_TRUE = true;
    public static final Boolean BOOLEAN_FALSE = false;

    // Load status constants - completed statuses
    public static final String LOAD_COMPLETED = "LOAD_COMPLETED";
    public static final String LOAD_COMMITTED_W_WRITE_CONFLICTS = "LOAD_COMMITTED_W_WRITE_CONFLICTS";

    // Load status constants - in-progress statuses
    public static final String LOAD_IN_PROGRESS = "LOAD_IN_PROGRESS";
    public static final String LOAD_STARTING = "LOAD_STARTING";
    public static final String LOAD_QUEUED = "LOAD_QUEUED";
    public static final String LOAD_COMMITTING = "LOAD_COMMITTING";

    // Load status constants - failure statuses
    public static final String LOAD_FAILED = "LOAD_FAILED";
    public static final String LOAD_CANCELLED = "LOAD_CANCELLED";
    public static final String LOAD_CANCELLED_BY_USER = "LOAD_CANCELLED_BY_USER";
    public static final String LOAD_CANCELLED_DUE_TO_ERRORS = "LOAD_CANCELLED_DUE_TO_ERRORS";
    public static final String LOAD_UNEXPECTED_ERROR = "LOAD_UNEXPECTED_ERROR";
    public static final String LOAD_S3_READ_ERROR = "LOAD_S3_READ_ERROR";
    public static final String LOAD_S3_ACCESS_DENIED_ERROR = "LOAD_S3_ACCESS_DENIED_ERROR";
    public static final String LOAD_DATA_DEADLOCK = "LOAD_DATA_DEADLOCK";
    public static final String LOAD_DATA_FAILED_DUE_TO_FEED_MODIFIED_OR_DELETED = "LOAD_DATA_FAILED_DUE_TO_FEED_MODIFIED_OR_DELETED";
    public static final String LOAD_FAILED_BECAUSE_DEPENDENCY_NOT_SATISFIED = "LOAD_FAILED_BECAUSE_DEPENDENCY_NOT_SATISFIED";
    public static final String LOAD_FAILED_INVALID_REQUEST = "LOAD_FAILED_INVALID_REQUEST";

    public static BulkLoadConfig createBulkLoadConfig(
            String bucket, String s3Prefix, String neptuneEndpoint, String iamRoleArn, String parallelism, boolean monitor) {
        BulkLoadConfig bulkLoadConfig = new BulkLoadConfig();
        bulkLoadConfig.setBucketName(bucket);
        bulkLoadConfig.setS3Prefix(s3Prefix);
        bulkLoadConfig.setNeptuneEndpoint(neptuneEndpoint);
        bulkLoadConfig.setIamRoleArn(iamRoleArn);
        bulkLoadConfig.setParallelism(parallelism);
        bulkLoadConfig.setMonitor(monitor);
        bulkLoadConfig.setCompress(false);
        bulkLoadConfig.setCompressDelete(false);
        return bulkLoadConfig;
    }

    public static NeptuneBulkLoader createNeptuneBulkLoader() {
        BulkLoadConfig bulkLoadConfig =
            createBulkLoadConfig(BUCKET, S3_PREFIX, NEPTUNE_ENDPOINT, IAM_ROLE_ARN, BULK_LOAD_PARALLELISM_MEDIUM, BOOLEAN_FALSE);
        try (NeptuneBulkLoader loader = new NeptuneBulkLoader(bulkLoadConfig)) {
            return loader;
        }
    }

    /**
     * Creates a NeptuneBulkLoader with custom HttpClient and S3TransferManager for testing
     * @param httpClient The HttpClient to use for HTTP requests
     * @param transferManager The S3TransferManager to use for S3 operations
     * @return NeptuneBulkLoader instance with the provided clients
     */
    public static NeptuneBulkLoader createNeptuneBulkLoader(HttpClient httpClient, S3TransferManager transferManager) {
        BulkLoadConfig bulkLoadConfig =
            createBulkLoadConfig(BUCKET, S3_PREFIX, NEPTUNE_ENDPOINT, IAM_ROLE_ARN, BULK_LOAD_PARALLELISM_MEDIUM, BOOLEAN_FALSE);
        return new NeptuneBulkLoader(
            bulkLoadConfig,
            httpClient,
            transferManager
        );
    }

    /**
     * Creates mock CSV files (both vertices and edges) in the specified directory
     * @param directory The directory where CSV files should be created
     * @param verticesFile The file location where verticies CSV data should be written
     * @param edgesFile The file location where edges CSV data should be written
     * @throws IOException If file creation fails
     */
    public static void createMockCsvFiles(File directory, File verticesFile, File edgesFile) throws IOException {
        createMockVerticesFile(directory, verticesFile);
        createMockEdgesFile(directory, edgesFile);
    }

    /**
     * Creates mock CSV files (both vertices and edges) in the specified directory
     * @param directory The directory where CSV files should be created
     * @throws IOException If file creation fails
     */
    public static void createMockCsvFiles(File directory) throws IOException {
        File testVerticiesFile = new File(directory, TestDataProvider.VERTICIES_CSV);
        File testEdgesFile = new File(directory, TestDataProvider.EDGES_CSV);
        createMockVerticesFile(directory, testVerticiesFile);
        createMockEdgesFile(directory, testEdgesFile);
    }

    /**
     * Creates a mock vertices.csv file with sample Neptune vertex data
     * @param directory The directory where the vertices.csv file should be created
     * @throws IOException If file creation fails
     */
    public static void createMockVerticesFile(File directory, File verticesFile) throws IOException {
        String verticesContent = "~id,~label,name,age\n" +
                                "v1,Person,John,30\n" +
                                "v2,Person,Jane,25\n" +
                                "v3,Company,ACME,null\n";
        Files.write(verticesFile.toPath(), verticesContent.getBytes());
    }

    /**
     * Creates a mock edges.csv file with sample Neptune edge data
     * @param directory The directory where the edges.csv file should be created
     * @throws IOException If file creation fails
     */
    public static void createMockEdgesFile(File directory, File edgesFile) throws IOException {
        String edgesContent = "~id,~from,~to,~label,weight\n" +
                             "e1,v1,v2,knows,0.8\n" +
                             "e2,v1,v3,works_for,1.0\n" +
                             "e3,v2,v3,works_for,1.0\n";
        Files.write(edgesFile.toPath(), edgesContent.getBytes());
    }

    private static final Supplier<String> ID_GENERATOR = () -> "edge-id";

    public static EdgeMetadata createEdgeMetadata(String columnHeaders) {
        return EdgeMetadata.parse(
                CSVUtils.firstRecord(columnHeaders),
                ID_GENERATOR,
                new PropertyValueParser(MultiValuedRelationshipPropertyPolicy.LeaveAsString, "", false),
                new ConversionConfig(), new HashSet<String>(), new HashMap<String, String>());
    }

    public static EdgeMetadata createEdgeMetadata(String columnHeaders, ConversionConfig conversionConfig, Set<String> skippedVertexIds, Map<String, String> vertexIdMap) {
        return EdgeMetadata.parse(
                CSVUtils.firstRecord(columnHeaders),
                ID_GENERATOR,
                new PropertyValueParser(MultiValuedRelationshipPropertyPolicy.LeaveAsString, "", false),
                conversionConfig, skippedVertexIds, vertexIdMap);
    }

    public static EdgeMetadata createEdgeMetadata(String columnHeaders, ConversionConfig conversionConfig, Set<String> skippedVertexIds) {
        return EdgeMetadata.parse(
                CSVUtils.firstRecord(columnHeaders),
                ID_GENERATOR,
                new PropertyValueParser(MultiValuedRelationshipPropertyPolicy.LeaveAsString, "", false),
                conversionConfig, skippedVertexIds, new HashMap<String, String>());
    }

    public static VertexMetadata createVertexMetadata(String columnHeaders, ConversionConfig config) {
        return VertexMetadata.parse(
                CSVUtils.firstRecord(columnHeaders),
                new PropertyValueParser(MultiValuedNodePropertyPolicy.PutInSetIgnoringDuplicates, "", false), config);
    }

    // ========== GZIP COMPRESSION TEST UTILITIES ==========

    /**
     * Test data constants for compression tests
     */
    public static final String SAMPLE_CSV_CONTENT_SIMPLE = "header1,header2\nvalue1,value2\nvalue3,value4";
    public static final String SAMPLE_CSV_CONTENT_NAMES = "name,age\nJohn,25\nJane,30";
    public static final String SAMPLE_CSV_CONTENT_SINGLE_COL = "col1,col2\nval1,val2";
    public static final String SAMPLE_CSV_CONTENT_HEADER_ONLY = "header\nvalue";
    public static final String SAMPLE_CSV_CONTENT_ID = "id\n123";
    public static final String SAMPLE_CSV_CONTENT_DATA = "data\nvalue";
    public static final String SAMPLE_CSV_CONTENT_EMPTY = "";
    public static final String SAMPLE_CSV_CONTENT_SINGLE_CHAR = "a";

    /**
     * Creates repetitive CSV content for compression ratio testing
     * @param rows Number of rows to generate
     * @return CSV content with repetitive data
     */
    public static String createRepetitiveCsvContent(int rows) {
        StringBuilder content = new StringBuilder();
        for (int i = 0; i < rows; i++) {
            content.append("same,data,repeated,multiple,times\n");
        }
        return content.toString();
    }

    /**
     * Creates large CSV content for buffer size testing
     * @param rows Number of rows to generate
     * @return CSV content with multiple columns
     */
    public static String createLargeCsvContent(int rows) {
        StringBuilder content = new StringBuilder();
        String row = "column1,column2,column3,column4,column5\n";
        for (int i = 0; i < rows; i++) {
            content.append(row);
        }
        return content.toString();
    }

    /**
     * Creates a test CSV file with specified content
     * @param directory The directory where the file should be created
     * @param filename The name of the CSV file
     * @param content The content to write to the file
     * @return The created File object
     * @throws IOException If file creation fails
     */
    public static File createTestCsvFile(File directory, String filename, String content) throws IOException {
        File csvFile = new File(directory, filename);
        try (java.io.FileWriter writer = new java.io.FileWriter(csvFile)) {
            writer.write(content);
        }
        return csvFile;
    }

    /**
     * Decompresses a GZIP file and returns its content as a string
     * @param gzipFile The GZIP file to decompress
     * @return The decompressed content as a string
     * @throws IOException If decompression fails
     */
    public static String decompressGzipFile(File gzipFile) throws IOException {
        StringBuilder content = new StringBuilder();
        try (java.io.FileInputStream fis = new java.io.FileInputStream(gzipFile);
             java.util.zip.GZIPInputStream gzis = new java.util.zip.GZIPInputStream(fis);
             java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(gzis))) {

            String line;
            boolean first = true;
            while ((line = reader.readLine()) != null) {
                if (!first) {
                    content.append("\n");
                }
                content.append(line);
                first = false;
            }
        }
        return content.toString();
    }

    /**
     * Captures System.err output for testing console output
     * @return ByteArrayOutputStream that captures the output
     */
    public static java.io.ByteArrayOutputStream captureSystemErr() {
        java.io.ByteArrayOutputStream capturedOutput = new java.io.ByteArrayOutputStream();
        System.setErr(new java.io.PrintStream(capturedOutput));
        return capturedOutput;
    }

    /**
     * Restores the original System.err
     * @param originalErr The original PrintStream to restore
     */
    public static void restoreSystemErr(java.io.PrintStream originalErr) {
        System.setErr(originalErr);
    }
}
