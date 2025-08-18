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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

/**
 * Utility class for gzip compression of CSV files
 */
public class GzipCompressUtils {
    /**
     * Upload Neptune vertices and edges CSV files asynchronously
     */
    public static void compressCsvFiles(String uploadDirectoryPath, Boolean isCompressDelete) throws Exception {
        System.err.println("Compressing .csv to .gzip...");

        File directory = new File(uploadDirectoryPath);
        if (!directory.exists() || !directory.isDirectory()) {
            throw new IllegalStateException("Directory does not exist: " + uploadDirectoryPath);
        }

        File[] csvFiles = directory.listFiles((dir, name) ->
            name.toLowerCase().endsWith(".csv"));

        if (csvFiles == null || csvFiles.length == 0) {
            System.err.println("No CSV files found in directory: " + uploadDirectoryPath);
            return;
        }

        System.err.println("Starting concurrent compression of " + csvFiles.length + " CSV files...");

        List<CompletableFuture<String>> compressionFutures = Arrays.stream(csvFiles)
            .map(file -> CompletableFuture.supplyAsync(() -> {
                try {
                    return compressSingleCsvFile(file.getAbsolutePath(), isCompressDelete);
                } catch (IOException e) {
                    System.err.println("Failed to compress " + file.getName() + ": " + e.getMessage());
                    return null; // Return null to indicate failure, will be filtered out
                }
            }))
            .collect(Collectors.toList());

        List<String> compressedFiles = compressionFutures.stream()
            .map(CompletableFuture::join)
            .filter(path -> path != null)
            .collect(Collectors.toList());

        // Check if any files failed to compress
        int failedCount = csvFiles.length - compressedFiles.size();
        if (failedCount > 0) {
            System.err.println("Warning: " + failedCount + " file(s) failed to compress");
        }

        System.err.println("Compression completed: " + compressedFiles.size() + "/" + csvFiles.length + " files successfully compressed");
    }

    /**
     * Compress a single CSV file with optimized buffer sizes for large files
     */
    private static String compressSingleCsvFile(String csvFilePath, Boolean isCompressDelete) throws IOException {
        File inputFile = new File(csvFilePath);
        String outputPath = csvFilePath + ".gz";

        System.err.println("Compressing: " + inputFile.getName() + " (" + Utils.formatFileSize(inputFile.length()) + ")");

        // Optimize buffer size based on file size
        int bufferSize = inputFile.length() > 1024 * 1024 * 1024 ? // > 1GB
            256 * 1024 : // 256KB for large files
            64 * 1024;   // 64KB for smaller files

        try (FileInputStream fis = new FileInputStream(csvFilePath);
            FileOutputStream fos = new FileOutputStream(outputPath);
            GZIPOutputStream gzipOut = new GZIPOutputStream(fos, bufferSize);
            BufferedInputStream bis = new BufferedInputStream(fis, bufferSize);
            BufferedOutputStream bos = new BufferedOutputStream(gzipOut, bufferSize)) {

            byte[] buffer = new byte[bufferSize];
            int bytesRead;
            long startTime = System.currentTimeMillis();

            while ((bytesRead = bis.read(buffer)) != -1) {
                bos.write(buffer, 0, bytesRead);
            }

            long compressionTime = System.currentTimeMillis() - startTime;
            File outputFile = new File(outputPath);
            double compressionRatio = (double) outputFile.length() / inputFile.length();

            System.err.println("✓ Compressed: " + inputFile.getName() +
                " -> " + outputFile.getName() +
                " (" + Utils.formatFileSize(outputFile.length()) +
                ", " + String.format("%.1f%%", compressionRatio * 100) + " of original" +
                ", " + String.format("%.1fs", compressionTime / 1000.0) + ")");

        }

        // Remove original file after successful compression only if isCompressDelete is true
        if (isCompressDelete) {
            if (inputFile.delete()) {
                System.err.println("Removed original file: " + inputFile.getName());
            } else {
                System.err.println("Warning: Could not remove original file: " + inputFile.getName());
            }
        } else {
            System.err.println("Original file retained: " + inputFile.getName());
        }

        return outputPath;
    }
}
