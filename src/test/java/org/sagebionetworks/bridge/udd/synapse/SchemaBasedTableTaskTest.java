package org.sagebionetworks.bridge.udd.synapse;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.util.Set;

import org.joda.time.LocalDate;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.workerPlatform.exceptions.AsyncTaskExecutionException;

@SuppressWarnings("unchecked")
public class SchemaBasedTableTaskTest {
    private static final LocalDate START_DATE = LocalDate.parse("2015-03-09");
    private static final LocalDate END_DATE = LocalDate.parse("2015-09-16");
    private static final String HEALTH_CODE = "test-health-code";
    private static final String SCHEMA_ID = "test-schema";
    private static final int SCHEMA_REV = 42;
    private static final String STUDY_ID = "test-study";
    private static final String TABLE_ID = "test-table-id";

    private static final UploadSchemaKey SCHEMA_KEY = new UploadSchemaKey.Builder().withAppId(STUDY_ID)
            .withSchemaId(SCHEMA_ID).withRevision(SCHEMA_REV).build();
    private static final UploadSchema SCHEMA = new UploadSchema.Builder().withKey(SCHEMA_KEY)
            .addField("foo", "INT")
            .addField("bar", "ATTACHMENT_BLOB")
            .addField("baz", "ATTACHMENT_V2").build();

    private InMemoryFileHelper inMemoryFileHelper;
    private DynamoHelper mockDynamoHelper;
    private SynapseHelper mockSynapseHelper;
    private SynapseDownloadFromTableTask task;
    private File tmpDir;

    @BeforeMethod
    public void before() {
        // Mock filehelper and temp dir.
        inMemoryFileHelper = new InMemoryFileHelper();
        tmpDir = inMemoryFileHelper.createTempDir();

        // Set up other mocks.
        mockDynamoHelper = mock(DynamoHelper.class);
        mockSynapseHelper = mock(SynapseHelper.class);

        // Set up params and task.
        SynapseDownloadFromTableParameters params = new SynapseDownloadFromTableParameters.Builder()
                .withSynapseTableId(TABLE_ID).withHealthCode(HEALTH_CODE).withStartDate(START_DATE)
                .withEndDate(END_DATE).withTempDir(tmpDir).withSchema(SCHEMA).withStudyId(STUDY_ID).build();

        task = new SchemaBasedTableTask(params);
        task.setDynamoHelper(mockDynamoHelper);
        task.setFileHelper(inMemoryFileHelper);
        task.setSynapseHelper(mockSynapseHelper);
    }

    @Test(expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "schema must be specified")
    public void paramsMustHaveSchema() {
        SynapseDownloadFromTableParameters params = new SynapseDownloadFromTableParameters.Builder()
                .withSynapseTableId(TABLE_ID).withHealthCode(HEALTH_CODE).withStartDate(START_DATE)
                .withEndDate(END_DATE).withTempDir(tmpDir).withSchema(null).withStudyId(STUDY_ID).build();
        new SchemaBasedTableTask(params);
    }

    @Test
    public void getAdditionalAttachmentColumnSet() {
        Set<String> columnSet = task.getAdditionalAttachmentColumnSet();
        assertEquals(columnSet.size(), 2);
        assertTrue(columnSet.contains("bar"));
        assertTrue(columnSet.contains("baz"));
    }

    @Test
    public void getDownloadFilenamePrefix() {
        String filePrefix = task.getDownloadFilenamePrefix();
        assertEquals(filePrefix, SCHEMA_KEY.toString());
    }

    @Test
    public void tableDoesntExist() throws Exception {
        // Mock getTable()
        when(mockSynapseHelper.getTable(TABLE_ID)).thenThrow(SynapseNotFoundException.class);

        // Execute (throws exception).
        try {
            task.call();
            fail("expected exception");
        } catch (AsyncTaskExecutionException ex) {
            assertEquals(ex.getMessage(), "Synapse table " + TABLE_ID + " for schema " +
                    SCHEMA_KEY.toString() + " no longer exists");
        }

        // Verify we delete the schema from the table mapping.
        verify(mockDynamoHelper).deleteSynapseTableIdMapping(SCHEMA_KEY);

        // Since we never download the CSV, we don't create any files other than the temp dir. Delete the temp dir and
        // then verify that the filehelper is empty.
        inMemoryFileHelper.deleteDir(tmpDir);
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    @Test
    public void errorVerifyingTable() throws Exception {
        // Mock getTable()
        TestSynapseException originalEx = new TestSynapseException("test exception");
        when(mockSynapseHelper.getTable(TABLE_ID)).thenThrow(originalEx);

        // Execute (throws exception).
        try {
            task.call();
            fail("expected exception");
        } catch (AsyncTaskExecutionException ex) {
            assertEquals(ex.getMessage(), "Error verifying synapse table " + TABLE_ID + " for schema " +
                    SCHEMA_KEY.toString() + ": test exception");
            assertSame(ex.getCause(), originalEx);
        }

        // Verify we _didn't_ delete the table mapping.
        verify(mockDynamoHelper, never()).deleteSynapseTableIdMapping(SCHEMA_KEY);

        // Since we never download the CSV, we don't create any files other than the temp dir. Delete the temp dir and
        // then verify that the filehelper is empty.
        inMemoryFileHelper.deleteDir(tmpDir);
        assertTrue(inMemoryFileHelper.isEmpty());
    }
}
