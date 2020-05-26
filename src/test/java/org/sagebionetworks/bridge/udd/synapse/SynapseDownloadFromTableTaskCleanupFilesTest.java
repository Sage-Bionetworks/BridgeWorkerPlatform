package org.sagebionetworks.bridge.udd.synapse;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.OutputStream;

import org.joda.time.LocalDate;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

// Deep tests for SynapseDownloadFromTableTask.cleanupFiles()
public class SynapseDownloadFromTableTaskCleanupFilesTest {
    private static final byte[] EMPTY_FILE_CONTENT = new byte[0];
    private static final UploadSchemaKey TEST_SCHEMA_KEY = new UploadSchemaKey.Builder().withAppId("test-study")
            .withSchemaId("test-schema").withRevision(42).build();
    private static final UploadSchema TEST_SCHEMA = new UploadSchema.Builder().withKey(TEST_SCHEMA_KEY)
            .addField("foo", "STRING").build();

    private File tmpDir;
    private InMemoryFileHelper inMemoryFileHelper;
    private SynapseDownloadFromTableTask task;

    @BeforeMethod
    public void setup() {
        // mock file helper and temp dir
        inMemoryFileHelper = new InMemoryFileHelper();
        tmpDir = inMemoryFileHelper.createTempDir();

        SynapseDownloadFromTableParameters params = new SynapseDownloadFromTableParameters.Builder()
                .withSynapseTableId("test-table-id").withHealthCode("test-health-code")
                .withStartDate(LocalDate.parse("2015-03-09")).withEndDate(LocalDate.parse("2015-09-16"))
                .withTempDir(tmpDir).withSchema(TEST_SCHEMA).build();
        task = new SynapseDownloadFromTableTask(params);
        task.setFileHelper(inMemoryFileHelper);
    }

    @Test
    public void noFiles() throws Exception {
        // Default context has all null files.
        executeTest();
    }

    @Test
    public void csvOnly() throws Exception {
        task.getContext().setCsvFile(createEmptyFile("csv.csv"));
        executeTest();
    }

    @Test
    public void csvAndBulkDownload() throws Exception {
        task.getContext().setCsvFile(createEmptyFile("csv.csv"));
        task.getContext().setBulkDownloadFile(createEmptyFile("download.zip"));
        executeTest();
    }

    @Test
    public void csvAndEditedCsv() throws Exception {
        task.getContext().setCsvFile(createEmptyFile("csv.csv"));
        task.getContext().setEditedCsvFile(createEmptyFile("csv-edited.csv"));
        executeTest();
    }

    @Test
    public void all3Files() throws Exception {
        task.getContext().setCsvFile(createEmptyFile("csv.csv"));
        task.getContext().setBulkDownloadFile(createEmptyFile("download.zip"));
        task.getContext().setEditedCsvFile(createEmptyFile("csv-edited.csv"));
        executeTest();
    }

    // branch coverage
    @Test
    public void nonNullFilesButDontExist() throws Exception {
        // Create the files, but don't write any content to them, so they won't exist.
        task.getContext().setCsvFile(inMemoryFileHelper.newFile(tmpDir, "csv.csv"));
        task.getContext().setBulkDownloadFile(inMemoryFileHelper.newFile(tmpDir, "download.zip"));
        task.getContext().setEditedCsvFile(inMemoryFileHelper.newFile(tmpDir, "csv-edited.csv"));
        executeTest();
    }

    // We don't want to use AfterMethod for this, because if it fails, TestNG won't tell us which test failed.
    public void executeTest() throws Exception {
        // Run the actual cleanup.
        task.cleanupFiles();

        // The only file remaining is the temp dir. Delete that dir, then verify the mock file system is empty.
        inMemoryFileHelper.deleteDir(tmpDir);
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    // Creates a trivial empty file, so we can test cleanup.
    private File createEmptyFile(String filename) throws Exception {
        File file = inMemoryFileHelper.newFile(tmpDir, filename);
        touchFile(file);
        return file;
    }

    // Write an empty string to the file to ensure that it exists in our (mock) file system.
    private void touchFile(File file) throws Exception {
        try (OutputStream fileOutputStream = inMemoryFileHelper.getOutputStream(file)) {
            fileOutputStream.write(EMPTY_FILE_CONTENT);
        }
    }
}
