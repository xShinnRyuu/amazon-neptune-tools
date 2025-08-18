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

import com.amazonaws.services.neptune.TestDataProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.*;

import static org.junit.Assert.*;

/**
 * Unit tests for GzipCompressUtils using Mockito and JUnit 4
 * Uses TestDataProvider for centralized test data and helper methods
 */
@RunWith(MockitoJUnitRunner.class)
public class GzipCompressUtilsTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private ByteArrayOutputStream capturedOutput;
    private PrintStream originalErr;

    @Before
    public void setUp() {
        originalErr = System.err;
        capturedOutput = TestDataProvider.captureSystemErr();
    }

    @After
    public void tearDown() {
        TestDataProvider.restoreSystemErr(originalErr);
    }

    @Test
    public void testCompressCsvFiles_WithValidCsvFiles() throws Exception {
        File testDir = tempFolder.getRoot();
        File csvFile1 = TestDataProvider.createTestCsvFile(testDir, "test1.csv", TestDataProvider.SAMPLE_CSV_CONTENT_SIMPLE);
        File csvFile2 = TestDataProvider.createTestCsvFile(testDir, "test2.csv", TestDataProvider.SAMPLE_CSV_CONTENT_NAMES);

        GzipCompressUtils.compressCsvFiles(testDir.getAbsolutePath(), true);

        File gzFile1 = new File(testDir, "test1.csv.gz");
        File gzFile2 = new File(testDir, "test2.csv.gz");
        assertTrue("Compressed file test1.csv.gz should exist", gzFile1.exists());
        assertTrue("Compressed file test2.csv.gz should exist", gzFile2.exists());

        assertFalse("Original CSV file test1.csv should be deleted", csvFile1.exists());
        assertFalse("Original CSV file test2.csv should be deleted", csvFile2.exists());

        String decompressed1 = TestDataProvider.decompressGzipFile(gzFile1);
        String decompressed2 = TestDataProvider.decompressGzipFile(gzFile2);
        assertEquals(TestDataProvider.SAMPLE_CSV_CONTENT_SIMPLE, decompressed1);
        assertEquals(TestDataProvider.SAMPLE_CSV_CONTENT_NAMES, decompressed2);

        String output = capturedOutput.toString();
        assertTrue(output.contains("Compressing .csv to .gzip..."));
        assertTrue(output.contains("Starting concurrent compression of 2 CSV files..."));
        assertTrue(output.contains("Compression completed: 2/2 files successfully compressed"));
        assertTrue(output.contains("✓ Compressed: test1.csv"));
        assertTrue(output.contains("✓ Compressed: test2.csv"));
        assertTrue(output.contains("Removed original file: test1.csv"));
        assertTrue(output.contains("Removed original file: test2.csv"));
    }

    @Test(expected = IllegalStateException.class)
    public void testCompressCsvFiles_WithNonExistentDirectory() throws Exception {
        String nonExistentDir = "/path/that/does/not/exist";
        GzipCompressUtils.compressCsvFiles(nonExistentDir, true);
    }

    @Test
    public void testCompressCsvFiles_WithNonExistentDirectory_CheckMessage() {
        String nonExistentDir = "/path/that/does/not/exist";
        try {
            GzipCompressUtils.compressCsvFiles(nonExistentDir, true);
            fail("Expected IllegalStateException to be thrown");
        } catch (IllegalStateException e) {
            assertEquals("Directory does not exist: " + nonExistentDir, e.getMessage());
        } catch (Exception e) {
            fail("Expected IllegalStateException but got: " + e.getClass().getSimpleName());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testCompressCsvFiles_WithFileInsteadOfDirectory() throws Exception {
        File testFile = tempFolder.newFile("notadirectory.txt");
        GzipCompressUtils.compressCsvFiles(testFile.getAbsolutePath(), true);
    }

    @Test
    public void testCompressCsvFiles_WithEmptyDirectory() throws Exception {
        File emptyDir = tempFolder.getRoot();
        GzipCompressUtils.compressCsvFiles(emptyDir.getAbsolutePath(), true);

        String output = capturedOutput.toString();
        assertTrue(output.contains("No CSV files found in directory:"));
        assertTrue(output.contains("Compressing .csv to .gzip..."));
    }

    @Test
    public void testCompressCsvFiles_WithNoCsvFiles() throws Exception {
        File testDir = tempFolder.getRoot();
        tempFolder.newFile("test.txt");
        tempFolder.newFile("data.json");
        GzipCompressUtils.compressCsvFiles(testDir.getAbsolutePath(), true);

        String output = capturedOutput.toString();
        assertTrue(output.contains("No CSV files found in directory:"));
    }

    @Test
    public void testCompressCsvFiles_WithMixedFileTypes() throws Exception {
        File testDir = tempFolder.getRoot();
        TestDataProvider.createTestCsvFile(testDir, "data.csv", TestDataProvider.SAMPLE_CSV_CONTENT_SINGLE_COL);
        tempFolder.newFile("readme.txt");
        tempFolder.newFile("config.json");
        GzipCompressUtils.compressCsvFiles(testDir.getAbsolutePath(), true);

        assertTrue(new File(testDir, "data.csv.gz").exists());
        assertFalse(new File(testDir, "data.csv").exists());

        assertTrue(new File(testDir, "readme.txt").exists());
        assertTrue(new File(testDir, "config.json").exists());

        String output = capturedOutput.toString();
        assertTrue(output.contains("Starting concurrent compression of 1 CSV files..."));
        assertTrue(output.contains("Compression completed: 1/1 files successfully compressed"));
    }

    @Test
    public void testCompressCsvFiles_WithLargeFile() throws Exception {
        File testDir = tempFolder.getRoot();
        String largeContent = TestDataProvider.createLargeCsvContent(1000);
        File largeCsvFile = TestDataProvider.createTestCsvFile(testDir, "large.csv", largeContent);
        GzipCompressUtils.compressCsvFiles(testDir.getAbsolutePath(), true);

        File compressedFile = new File(testDir, "large.csv.gz");
        assertTrue(compressedFile.exists());
        assertFalse(largeCsvFile.exists());

        assertTrue("Compressed file should be smaller than original",
                  compressedFile.length() < largeContent.length());

        String output = capturedOutput.toString();
        assertTrue(output.contains("✓ Compressed: large.csv"));
    }

    @Test
    public void testCompressCsvFiles_WithCaseInsensitiveExtensions() throws Exception {
        File testDir = tempFolder.getRoot();
        TestDataProvider.createTestCsvFile(testDir, "data.CSV", TestDataProvider.SAMPLE_CSV_CONTENT_HEADER_ONLY);
        TestDataProvider.createTestCsvFile(testDir, "info.Csv", "name\ntest");
        TestDataProvider.createTestCsvFile(testDir, "report.cSv", TestDataProvider.SAMPLE_CSV_CONTENT_ID);
        GzipCompressUtils.compressCsvFiles(testDir.getAbsolutePath(), true);

        assertTrue(new File(testDir, "data.CSV.gz").exists());
        assertTrue(new File(testDir, "info.Csv.gz").exists());
        assertTrue(new File(testDir, "report.cSv.gz").exists());

        String output = capturedOutput.toString();
        assertTrue(output.contains("Starting concurrent compression of 3 CSV files..."));
        assertTrue(output.contains("Compression completed: 3/3 files successfully compressed"));
    }

    @Test
    public void testCompressCsvFiles_WithReadOnlyFile() throws Exception {
        File testDir = tempFolder.getRoot();
        File csvFile = TestDataProvider.createTestCsvFile(testDir, "readonly.csv", TestDataProvider.SAMPLE_CSV_CONTENT_DATA);

        csvFile.setReadOnly();

        GzipCompressUtils.compressCsvFiles(testDir.getAbsolutePath(), true);

        File compressedFile = new File(testDir, "readonly.csv.gz");
        assertTrue("Compressed file should exist", compressedFile.exists());

        String output = capturedOutput.toString();
        // System-agnostic: check for either successful deletion or warning message
        // Different operating systems handle read-only files differently
        assertTrue("Should contain either deletion success or warning message",
                  output.contains("Removed original file: readonly.csv") ||
                  output.contains("Warning: Could not remove original file: readonly.csv"));
    }

    @Test
    public void testCompressCsvFiles_VerifyCompressionRatio() throws Exception {
        File testDir = tempFolder.getRoot();
        String repetitiveContent = TestDataProvider.createRepetitiveCsvContent(100);
        File csvFile = TestDataProvider.createTestCsvFile(testDir, "repetitive.csv", repetitiveContent);
        long originalSize = csvFile.length();
        GzipCompressUtils.compressCsvFiles(testDir.getAbsolutePath(), true);

        File compressedFile = new File(testDir, "repetitive.csv.gz");
        assertTrue(compressedFile.exists());

        long compressedSize = compressedFile.length();
        double compressionRatio = (double) compressedSize / originalSize;

        // Repetitive content should compress to less than 50% of original size
        assertTrue("Compression ratio should be less than 50% for repetitive content, was: " +
                  String.format("%.1f%%", compressionRatio * 100),
                  compressionRatio < 0.5);

        String output = capturedOutput.toString();
        assertTrue(output.contains("% of original"));
    }

    @Test
    public void testCompressCsvFiles_EmptyFile() throws Exception {
        File testDir = tempFolder.getRoot();
        TestDataProvider.createTestCsvFile(testDir, "empty.csv", TestDataProvider.SAMPLE_CSV_CONTENT_EMPTY);
        GzipCompressUtils.compressCsvFiles(testDir.getAbsolutePath(), true);

        File compressedFile = new File(testDir, "empty.csv.gz");
        assertTrue("Compressed empty file should exist", compressedFile.exists());
        assertTrue("Compressed empty file should have some size (gzip header)", compressedFile.length() > 0);

        // Verify decompression works
        String decompressed = TestDataProvider.decompressGzipFile(compressedFile);
        assertEquals(TestDataProvider.SAMPLE_CSV_CONTENT_EMPTY, decompressed);
    }

    @Test
    public void testCompressCsvFiles_SingleCharacterFile() throws Exception {
        File testDir = tempFolder.getRoot();
        TestDataProvider.createTestCsvFile(testDir, "single.csv", TestDataProvider.SAMPLE_CSV_CONTENT_SINGLE_CHAR);
        GzipCompressUtils.compressCsvFiles(testDir.getAbsolutePath(), true);

        File compressedFile = new File(testDir, "single.csv.gz");
        assertTrue(compressedFile.exists());

        String decompressed = TestDataProvider.decompressGzipFile(compressedFile);
        assertEquals(TestDataProvider.SAMPLE_CSV_CONTENT_SINGLE_CHAR, decompressed);
    }

    @Test
    public void testCompressCsvFiles_WithRetainOriginalFiles() throws Exception {
        File testDir = tempFolder.getRoot();
        File csvFile1 = TestDataProvider.createTestCsvFile(testDir, "test1.csv", TestDataProvider.SAMPLE_CSV_CONTENT_SIMPLE);
        File csvFile2 = TestDataProvider.createTestCsvFile(testDir, "test2.csv", TestDataProvider.SAMPLE_CSV_CONTENT_NAMES);

        // Test with isCompressDelete = false to retain original files
        GzipCompressUtils.compressCsvFiles(testDir.getAbsolutePath(), false);

        File gzFile1 = new File(testDir, "test1.csv.gz");
        File gzFile2 = new File(testDir, "test2.csv.gz");
        assertTrue("Compressed file test1.csv.gz should exist", gzFile1.exists());
        assertTrue("Compressed file test2.csv.gz should exist", gzFile2.exists());

        // Original files should still exist when isCompressDelete = false
        assertTrue("Original CSV file test1.csv should be retained", csvFile1.exists());
        assertTrue("Original CSV file test2.csv should be retained", csvFile2.exists());

        String output = capturedOutput.toString();
        assertTrue(output.contains("Original file retained: test1.csv"));
        assertTrue(output.contains("Original file retained: test2.csv"));
    }

    @Test
    public void testCompressCsvFiles_WithSuccessfulFileDeletion() throws Exception {
        File testDir = tempFolder.getRoot();
        File csvFile = TestDataProvider.createTestCsvFile(testDir, "deletable.csv", TestDataProvider.SAMPLE_CSV_CONTENT_SIMPLE);

        // Ensure file is writable and deletable
        assertTrue("File should be writable", csvFile.canWrite());
        assertTrue("File should exist before compression", csvFile.exists());

        // Test with isCompressDelete = true to delete original files
        GzipCompressUtils.compressCsvFiles(testDir.getAbsolutePath(), true);

        File compressedFile = new File(testDir, "deletable.csv.gz");
        assertTrue("Compressed file should exist", compressedFile.exists());
        assertFalse("Original file should be deleted", csvFile.exists());

        String output = capturedOutput.toString();
        assertTrue("Should show successful deletion message",
                  output.contains("Removed original file: deletable.csv"));
    }

    @Test
    public void testCompressCsvFiles_WithFileDeletionScenario() throws Exception {
        File testDir = tempFolder.getRoot();
        File csvFile = TestDataProvider.createTestCsvFile(testDir, "test.csv", TestDataProvider.SAMPLE_CSV_CONTENT_SIMPLE);

        // Make the file read-only (behavior varies by OS)
        csvFile.setReadOnly();

        // Test with isCompressDelete = true
        GzipCompressUtils.compressCsvFiles(testDir.getAbsolutePath(), true);

        File compressedFile = new File(testDir, "test.csv.gz");
        assertTrue("Compressed file should exist", compressedFile.exists());

        String output = capturedOutput.toString();
        // System-agnostic: verify that appropriate deletion message appears
        // Either successful deletion or warning about failure
        assertTrue("Should show either successful deletion or warning message",
                  output.contains("Removed original file: test.csv") ||
                  output.contains("Warning: Could not remove original file: test.csv"));

        // Verify compression completed message appears regardless
        assertTrue("Should show compression completion",
                  output.contains("Compression completed: 1/1 files successfully compressed"));
    }

    @Test
    public void testCompressCsvFiles_WithMultipleFiles() throws Exception {
        File testDir = tempFolder.getRoot();

        // Create multiple files with different characteristics
        File normalFile = TestDataProvider.createTestCsvFile(testDir, "normal.csv", TestDataProvider.SAMPLE_CSV_CONTENT_SIMPLE);
        File anotherFile = TestDataProvider.createTestCsvFile(testDir, "another.csv", TestDataProvider.SAMPLE_CSV_CONTENT_NAMES);

        // Make one file read-only (behavior varies by OS)
        anotherFile.setReadOnly();

        // Test with isCompressDelete = true
        GzipCompressUtils.compressCsvFiles(testDir.getAbsolutePath(), true);

        // Check compressed files exist
        assertTrue("Normal compressed file should exist", new File(testDir, "normal.csv.gz").exists());
        assertTrue("Another compressed file should exist", new File(testDir, "another.csv.gz").exists());

        String output = capturedOutput.toString();

        // Verify compression messages appear
        assertTrue("Should show compression started",
                  output.contains("Starting concurrent compression of 2 CSV files..."));
        assertTrue("Should show compression completed",
                  output.contains("Compression completed: 2/2 files successfully compressed"));

        // System-agnostic: verify deletion messages appear (either success or warning)
        assertTrue("Should show deletion message for normal file",
                  output.contains("Removed original file: normal.csv") ||
                  output.contains("Warning: Could not remove original file: normal.csv"));
        assertTrue("Should show deletion message for another file",
                  output.contains("Removed original file: another.csv") ||
                  output.contains("Warning: Could not remove original file: another.csv"));
    }

    @Test
    public void testCompressCsvFiles_WithCompressionFailure() throws Exception {
        File testDir = tempFolder.getRoot();

        // Create a valid CSV file
        TestDataProvider.createTestCsvFile(testDir, "valid.csv", TestDataProvider.SAMPLE_CSV_CONTENT_SIMPLE);

        // Create a directory with the same name as a CSV file to cause IOException
        File invalidCsvDir = new File(testDir, "invalid.csv");
        assertTrue("Should create directory", invalidCsvDir.mkdir());

        // This should handle the failure gracefully
        GzipCompressUtils.compressCsvFiles(testDir.getAbsolutePath(), true);

        String output = capturedOutput.toString();

        // Should show compression started
        assertTrue("Should show compression started", output.contains("Starting concurrent compression of 2 CSV files..."));

        // Should show failure for the invalid file
        assertTrue("Should show compression failure", output.contains("Failed to compress invalid.csv"));

        // Should show warning about failed files
        assertTrue("Should show warning about failed files", output.contains("Warning: 1 file(s) failed to compress"));

        // Should show completion with partial success
        assertTrue("Should show partial completion", output.contains("Compression completed: 1/2 files successfully compressed"));

        // Valid file should be compressed successfully
        File validGzFile = new File(testDir, "valid.csv.gz");
        assertTrue("Valid file should be compressed", validGzFile.exists());

        // Invalid "file" (directory) should not have a .gz version
        File invalidGzFile = new File(testDir, "invalid.csv.gz");
        assertFalse("Invalid file should not be compressed", invalidGzFile.exists());
    }
}
