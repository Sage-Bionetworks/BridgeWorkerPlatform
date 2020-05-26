package org.sagebionetworks.bridge.udd.synapse;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import org.joda.time.LocalDate;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.file.BulkFileDownloadResponse;
import org.sagebionetworks.repo.model.file.FileDownloadSummary;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;
import org.sagebionetworks.bridge.udd.exceptions.AsyncTaskExecutionException;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SynapseDownloadFromTableTaskTest {
    private static final UploadSchemaKey TEST_SCHEMA_KEY = new UploadSchemaKey.Builder().withAppId("test-study")
            .withSchemaId("test-schema").withRevision(42).build();

    // The default test schema should include at least 2 file handle IDs and a mix of file handles and
    // non-file-handles.
    private static final UploadSchema DEFAULT_TEST_SCHEMA = new UploadSchema.Builder().withKey(TEST_SCHEMA_KEY)
            .addField("foo", "INT").addField("bar", "ATTACHMENT_BLOB").addField("baz", "ATTACHMENT_JSON_BLOB").build();

    private InMemoryFileHelper inMemoryFileHelper;
    private ArgumentCaptor<String> synapseQueryCaptor;
    private ArgumentCaptor<Set> synapseFileHandleIdSetCaptor;
    private SynapseDownloadFromTableTask task;
    private File tmpDir;

    @Test
    public void csvHasNoUserRows() throws Exception {
        // setup
        String csvContent = "\"recordId\",\"healthCode\",\"foo\",\"bar\",\"baz\"";
        setupTestWithArgs(DEFAULT_TEST_SCHEMA, csvContent, null, null);

        // execute and validate
        SynapseDownloadFromTableResult result = task.call();
        assertNull(result.getCsvFile());
        assertNull(result.getBulkDownloadFile());
        postValidation(result);
    }

    @Test
    public void csvHasNoHealthCode() throws Exception {
        // setup
        String csvContent = "\"recordId\",\"foo\",\"bar\",\"baz\"\n" +
                "\"record-1\",\"37\",\"file-handle-1\",\"file-handle-2\"";
        setupTestWithArgs(DEFAULT_TEST_SCHEMA, csvContent, null, null);

        // execute
        Exception thrownEx = null;
        try {
            task.call();
            fail("expected exception");
        } catch (IllegalStateException ex) {
            thrownEx = ex;
        }
        assertTrue(thrownEx.getMessage().contains("healthCodeColumnIndex"));

        // validate
        postValidation(null);
    }

    @Test
    public void schemaHasNoFileHandles() throws Exception {
        // setup
        UploadSchema schema = new UploadSchema.Builder().withKey(TEST_SCHEMA_KEY).addField("asdf", "INT").build();
        String csvContent = "\"recordId\",\"healthCode\",\"asdf\"\n" +
                "\"record-1\",\"test-health-code\",\"7\"";
        setupTestWithArgs(schema, csvContent, null, null);

        // execute and validate
        SynapseDownloadFromTableResult result = task.call();
        assertNull(result.getBulkDownloadFile());
        List<String[]> parsedCsv = parseCsv(result.getCsvFile());
        assertEquals(parsedCsv.size(), 2);

        // header
        assertEquals(parsedCsv.get(0).length, 3);
        assertEquals(parsedCsv.get(0)[0], "recordId");
        assertEquals(parsedCsv.get(0)[1], "healthCode");
        assertEquals(parsedCsv.get(0)[2], "asdf");

        // row 1
        assertEquals(parsedCsv.get(1).length, 3);
        assertEquals(parsedCsv.get(1)[0], "record-1");
        assertTrue(Strings.isNullOrEmpty(parsedCsv.get(1)[1]));
        assertEquals(parsedCsv.get(1)[2], "7");

        postValidation(result);
    }

    @Test
    public void csvHasNoFileHandles() throws Exception {
        // setup
        String csvContent = "\"recordId\",\"healthCode\",\"foo\",\"bar\",\"baz\"\n" +
                "\"record-1\",\"test-health-code\",\"13\",,";
        setupTestWithArgs(DEFAULT_TEST_SCHEMA, csvContent, null, null);

        // execute and validate
        SynapseDownloadFromTableResult result = task.call();
        assertNull(result.getBulkDownloadFile());
        List<String[]> parsedCsv = parseCsv(result.getCsvFile());
        assertEquals(parsedCsv.size(), 2);

        // header
        assertEquals(parsedCsv.get(0).length, 5);
        assertEquals(parsedCsv.get(0)[0], "recordId");
        assertEquals(parsedCsv.get(0)[1], "healthCode");
        assertEquals(parsedCsv.get(0)[2], "foo");
        assertEquals(parsedCsv.get(0)[3], "bar");
        assertEquals(parsedCsv.get(0)[4], "baz");

        // row 1
        assertEquals(parsedCsv.get(1).length, 5);
        assertEquals(parsedCsv.get(1)[0], "record-1");
        assertTrue(Strings.isNullOrEmpty(parsedCsv.get(1)[1]));
        assertEquals(parsedCsv.get(1)[2], "13");
        assertTrue(Strings.isNullOrEmpty(parsedCsv.get(1)[3]));
        assertTrue(Strings.isNullOrEmpty(parsedCsv.get(1)[4]));

        postValidation(result);
    }

    @Test
    public void csvAndBulkDownload() throws Exception {
        // For full branch coverage, we need the following cases:
        // * row with no file handles
        // * row with 1 file handle
        // * row with 2 file handles
        // * file summary with null file handle id
        // * file summary with failure message
        // * file summary with unknown error
        // * missing file summary (unknown error)
        // * extraneous file summary

        // setup
        String csvContent = "\"recordId\",\"healthCode\",\"foo\",\"bar\",\"baz\"\n" +
                "\"record-1\",\"test-health-code\",\"4\",,\n" +
                "\"record-2\",\"test-health-code\",\"8\",\"file-handle-2a\",\n" +
                "\"record-3\",\"test-health-code\",\"15\",\"file-handle-3a\",\"file-handle-3b\"\n" +
                "\"record-4\",\"test-health-code\",\"16\",\"service-error-file-handle\",\"unknown-error-file-handle\"\n" +
                "\"record-5\",\"test-health-code\",\"23\",\"missing-file-handle\",";

        List<FileDownloadSummary> fileSummaryList = new ArrayList<>();
        {
            FileDownloadSummary fileSummary = new FileDownloadSummary();
            fileSummary.setFileHandleId("file-handle-2a");
            fileSummary.setZipEntryName("zip-entry-2a");
            fileSummaryList.add(fileSummary);
        }
        {
            FileDownloadSummary fileSummary = new FileDownloadSummary();
            fileSummary.setFileHandleId("file-handle-3a");
            fileSummary.setZipEntryName("zip-entry-3a");
            fileSummaryList.add(fileSummary);
        }
        {
            FileDownloadSummary fileSummary = new FileDownloadSummary();
            fileSummary.setFileHandleId("file-handle-3b");
            fileSummary.setZipEntryName("zip-entry-3b");
            fileSummaryList.add(fileSummary);
        }
        {
            FileDownloadSummary fileSummary = new FileDownloadSummary();
            fileSummary.setFailureMessage("You should never see this message");
            fileSummaryList.add(fileSummary);
        }
        {
            FileDownloadSummary fileSummary = new FileDownloadSummary();
            fileSummary.setFileHandleId("service-error-file-handle");
            fileSummary.setFailureMessage("service error");
            fileSummaryList.add(fileSummary);
        }
        {
            FileDownloadSummary fileSummary = new FileDownloadSummary();
            fileSummary.setFileHandleId("unknown-error-file-handle");
            fileSummaryList.add(fileSummary);
        }
        {
            FileDownloadSummary fileSummary = new FileDownloadSummary();
            fileSummary.setFileHandleId("extraneous-file-handle");
            fileSummary.setZipEntryName("extraneous-zip-entry");
            fileSummaryList.add(fileSummary);
        }

        setupTestWithArgs(DEFAULT_TEST_SCHEMA, csvContent, null, fileSummaryList);

        // execute
        SynapseDownloadFromTableResult result = task.call();

        // validate CSV
        List<String[]> parsedCsv = parseCsv(result.getCsvFile());
        assertEquals(parsedCsv.size(), 6);

        // header
        assertEquals(parsedCsv.get(0).length, 5);
        assertEquals(parsedCsv.get(0)[0], "recordId");
        assertEquals(parsedCsv.get(0)[1], "healthCode");
        assertEquals(parsedCsv.get(0)[2], "foo");
        assertEquals(parsedCsv.get(0)[3], "bar");
        assertEquals(parsedCsv.get(0)[4], "baz");

        // row 1
        assertEquals(parsedCsv.get(1).length, 5);
        assertEquals(parsedCsv.get(1)[0], "record-1");
        assertTrue(Strings.isNullOrEmpty(parsedCsv.get(1)[1]));
        assertEquals(parsedCsv.get(1)[2], "4");
        assertTrue(Strings.isNullOrEmpty(parsedCsv.get(1)[3]));
        assertTrue(Strings.isNullOrEmpty(parsedCsv.get(1)[4]));

        // row 2
        assertEquals(parsedCsv.get(2).length, 5);
        assertEquals(parsedCsv.get(2)[0], "record-2");
        assertTrue(Strings.isNullOrEmpty(parsedCsv.get(2)[1]));
        assertEquals(parsedCsv.get(2)[2], "8");
        assertEquals(parsedCsv.get(2)[3], "zip-entry-2a");
        assertTrue(Strings.isNullOrEmpty(parsedCsv.get(2)[4]));

        // row 3
        assertEquals(parsedCsv.get(3).length, 5);
        assertEquals(parsedCsv.get(3)[0], "record-3");
        assertTrue(Strings.isNullOrEmpty(parsedCsv.get(3)[1]));
        assertEquals(parsedCsv.get(3)[2], "15");
        assertEquals(parsedCsv.get(3)[3], "zip-entry-3a");
        assertEquals(parsedCsv.get(3)[4], "zip-entry-3b");

        // row 4 - For the unknown error, instead of string matching, just verify that the error message exists and
        // that it's not the same as the file handle ID.
        assertEquals(parsedCsv.get(4).length, 5);
        assertEquals(parsedCsv.get(4)[0], "record-4");
        assertTrue(Strings.isNullOrEmpty(parsedCsv.get(4)[1]));
        assertEquals(parsedCsv.get(4)[2], "16");
        assertEquals(parsedCsv.get(4)[3], "service error");
        assertNotEquals(parsedCsv.get(4)[4], "unknown-error-file-handle");
        assertFalse(Strings.isNullOrEmpty(parsedCsv.get(4)[4]));

        // row 5 - For the missing file handle, similar idea as the unknown error.
        assertEquals(parsedCsv.get(5).length, 5);
        assertEquals(parsedCsv.get(5)[0], "record-5");
        assertTrue(Strings.isNullOrEmpty(parsedCsv.get(5)[1]));
        assertEquals(parsedCsv.get(5)[2], "23");
        assertNotEquals(parsedCsv.get(5)[3], "missing-file-handle");
        assertFalse(Strings.isNullOrEmpty(parsedCsv.get(5)[3]));
        assertTrue(Strings.isNullOrEmpty(parsedCsv.get(5)[4]));

        // validate bulk download file - It's just "dummy zip content"
        try (Reader bulkDownloadFileReader = inMemoryFileHelper.getReader(result.getBulkDownloadFile())) {
            assertEquals(CharStreams.toString(bulkDownloadFileReader), "dummy zip content");
        }

        // validate the file handles we sent to Synapse for the bulk download
        Set<String> fileHandleIdSet = synapseFileHandleIdSetCaptor.getValue();
        assertEquals(fileHandleIdSet.size(), 6);
        assertTrue(fileHandleIdSet.contains("file-handle-2a"));
        assertTrue(fileHandleIdSet.contains("file-handle-3a"));
        assertTrue(fileHandleIdSet.contains("file-handle-3b"));
        assertTrue(fileHandleIdSet.contains("service-error-file-handle"));
        assertTrue(fileHandleIdSet.contains("unknown-error-file-handle"));
        assertTrue(fileHandleIdSet.contains("missing-file-handle"));

        postValidation(result);
    }

    @Test
    public void firstErrorCase() throws Exception {
        // Test getting an error on the first step (download CSV). This allows us to test that cleanup works even when
        // almost everything is null.

        // setup
        setupTestWithArgs(DEFAULT_TEST_SCHEMA, null, new TestSynapseException(), null);

        // execute
        Exception thrownEx = null;
        try {
            task.call();
            fail("expected exception");
        } catch (AsyncTaskExecutionException ex) {
            thrownEx = ex;
        }
        assertNotNull(thrownEx);
        postValidation(null);
    }

    @Test
    public void lastErrorCase() throws Exception {
        // Test getting an error on the last step (editing CSV). This allows us to test full cleanup, including the
        // bulk download file. Since the editCsv() step has no mock dependencies, we'll use a spy to get it to throw an
        // exception.

        // setup
        String csvContent = "\"recordId\",\"healthCode\",\"foo\",\"bar\",\"baz\"\n" +
                "\"record-1\",\"test-health-code\",\"1337\",\"test-file-handle\",";

        FileDownloadSummary fileSummary = new FileDownloadSummary();
        fileSummary.setFileHandleId("test-file-handle");
        fileSummary.setZipEntryName("test-zip-entry");

        setupTestWithArgs(DEFAULT_TEST_SCHEMA, csvContent, null, ImmutableList.of(fileSummary));

        task = spy(task);
        doThrow(new AsyncTaskExecutionException()).when(task).editCsv();

        // execute
        Exception thrownEx = null;
        try {
            task.call();
            fail("expected exception");
        } catch (AsyncTaskExecutionException ex) {
            thrownEx = ex;
        }
        assertNotNull(thrownEx);

        // validate the file handles we sent to Synapse for the bulk download
        Set<String> fileHandleIdSet = synapseFileHandleIdSetCaptor.getValue();
        assertEquals(fileHandleIdSet.size(), 1);
        assertTrue(fileHandleIdSet.contains("test-file-handle"));

        postValidation(null);
    }

    private void setupTestWithArgs(UploadSchema schema, String csvContent, SynapseException csvException,
            List<FileDownloadSummary> fileSummaryList) throws Exception {
        // mock file helper and temp dir
        inMemoryFileHelper = new InMemoryFileHelper();
        tmpDir = inMemoryFileHelper.createTempDir();

        // set up params and task
        SynapseDownloadFromTableParameters params = new SynapseDownloadFromTableParameters.Builder()
                .withSynapseTableId("test-table-id").withHealthCode("test-health-code")
                .withStartDate(LocalDate.parse("2015-03-09")).withEndDate(LocalDate.parse("2015-09-16"))
                .withTempDir(tmpDir).withSchema(schema).build();
        task = new SynapseDownloadFromTableTask(params);
        task.setFileHelper(inMemoryFileHelper);

        // mock Synapse CSV content
        SynapseHelper mockSynapseHelper = mock(SynapseHelper.class);
        synapseQueryCaptor = ArgumentCaptor.forClass(String.class);
        when(mockSynapseHelper.generateFileHandleFromTableQuery(synapseQueryCaptor.capture(), eq("test-table-id")))
                .thenReturn("query-csv-file-handle-id");
        doAnswer(invocation -> {
            if (csvException != null) {
                throw csvException;
            }

            File targetFile = invocation.getArgumentAt(1, File.class);
            try (Writer targetFileWriter = inMemoryFileHelper.getWriter(targetFile)) {
                targetFileWriter.write(csvContent);
            }

            // Needed because Answer declares a return type, even if it's null.
            return null;
        }).when(mockSynapseHelper).downloadFileHandle(eq("query-csv-file-handle-id"), any(File.class));

        if (fileSummaryList != null) {
            // mock Synapse bulk download
            BulkFileDownloadResponse bulkDownloadResponse = new BulkFileDownloadResponse();
            bulkDownloadResponse.setResultZipFileHandleId("bulk-download-file-handle-id");
            bulkDownloadResponse.setFileSummary(fileSummaryList);

            synapseFileHandleIdSetCaptor = ArgumentCaptor.forClass(Set.class);
            when(mockSynapseHelper.generateBulkDownloadFileHandle(eq("test-table-id"),
                    synapseFileHandleIdSetCaptor.capture())).thenReturn(bulkDownloadResponse);

            doAnswer(invocation -> {
                File targetFile = invocation.getArgumentAt(1, File.class);
                try (Writer targetFileWriter = inMemoryFileHelper.getWriter(targetFile)) {
                    targetFileWriter.write("dummy zip content");
                }

                // Needed because Answer declares a return type, even if it's null.
                return null;
            }).when(mockSynapseHelper).downloadFileHandle(eq("bulk-download-file-handle-id"), any(File.class));
        }

        task.setSynapseHelper(mockSynapseHelper);
    }

    // Direct string matching means we tightly couple to the CSV writer implementation. Instead, re-parse the file as a
    // CSV and check the values are what we expect.
    private List<String[]> parseCsv(File csvFile) throws Exception {
        try (CSVReader csvFileReader = new CSVReader(inMemoryFileHelper.getReader(csvFile))) {
            return csvFileReader.readAll();
        }
    }

    private void postValidation(SynapseDownloadFromTableResult result) throws Exception {
        // SynapseDownloadFromTableTask should only leave behind the files it returned and the temp dir. Clean these
        // files up (which is what the packager would do) and then verify that the mock file system is now empty.
        if (result != null) {
            if (result.getCsvFile() != null) {
                inMemoryFileHelper.deleteFile(result.getCsvFile());
            }

            if (result.getBulkDownloadFile() != null) {
                inMemoryFileHelper.deleteFile(result.getBulkDownloadFile());
            }
        }

        inMemoryFileHelper.deleteDir(tmpDir);
        assertTrue(inMemoryFileHelper.isEmpty());

        // Validate the Synapse query contains the expected values. Don't string match the entire string. Just validate
        // that table ID, health code, start date, and end date were used.
        String query = synapseQueryCaptor.getValue();
        assertTrue(query.contains("test-table-id"));
        assertTrue(query.contains("test-health-code"));
        assertTrue(query.contains("2015-03-09"));
        assertTrue(query.contains("2015-09-16"));
    }
}
