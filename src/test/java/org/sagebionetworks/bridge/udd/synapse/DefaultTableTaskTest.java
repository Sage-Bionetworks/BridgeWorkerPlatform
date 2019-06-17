package org.sagebionetworks.bridge.udd.synapse;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.util.Set;

import org.joda.time.LocalDate;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;
import org.sagebionetworks.client.exceptions.SynapseServerException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.udd.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.udd.exceptions.AsyncTaskExecutionException;

@SuppressWarnings("unchecked")
public class DefaultTableTaskTest {
    private static final LocalDate START_DATE = LocalDate.parse("2015-03-09");
    private static final LocalDate END_DATE = LocalDate.parse("2015-09-16");
    private static final String HEALTH_CODE = "test-health-code";
    private static final String STUDY_ID = "test-study";
    private static final String TABLE_ID = "test-table-id";

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
                .withEndDate(END_DATE).withTempDir(tmpDir).withStudyId(STUDY_ID).build();

        task = new DefaultTableTask(params);
        task.setDynamoHelper(mockDynamoHelper);
        task.setFileHelper(inMemoryFileHelper);
        task.setSynapseHelper(mockSynapseHelper);
    }

    @Test
    public void getAdditionalAttachmentColumnSet() {
        Set<String> columnSet = task.getAdditionalAttachmentColumnSet();
        assertTrue(columnSet.isEmpty());
    }

    @Test
    public void getDownloadFilenamePrefix() {
        String filePrefix = task.getDownloadFilenamePrefix();
        assertEquals(filePrefix, STUDY_ID + "-default");
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
            assertEquals(ex.getMessage(), "Synapse table " + TABLE_ID + " for default schema for study " +
                    STUDY_ID + " no longer exists");
        }

        // Verify we delete the table from the table mapping.
        verify(mockDynamoHelper).deleteDefaultSynapseTableForStudy(STUDY_ID);

        // Since we never download the CSV, we don't create any files other than the temp dir. Delete the temp dir and
        // then verify that the filehelper is empty.
        inMemoryFileHelper.deleteDir(tmpDir);
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    @Test
    public void errorVerifyingTable() throws Exception {
        // Mock getTable()
        when(mockSynapseHelper.getTable(TABLE_ID)).thenThrow(SynapseServerException.class);

        // Execute (throws exception).
        try {
            task.call();
            fail("expected exception");
        } catch (AsyncTaskExecutionException ex) {
            assertEquals(ex.getMessage(), "Error verifying synapse table " + TABLE_ID +
                    " for default schema for study " + STUDY_ID);
        }

        // Verify we _didn't_ delete the table mapping.
        verify(mockDynamoHelper, never()).deleteDefaultSynapseTableForStudy(STUDY_ID);

        // Since we never download the CSV, we don't create any files other than the temp dir. Delete the temp dir and
        // then verify that the filehelper is empty.
        inMemoryFileHelper.deleteDir(tmpDir);
        assertTrue(inMemoryFileHelper.isEmpty());
    }
}
