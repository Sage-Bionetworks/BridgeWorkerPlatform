package org.sagebionetworks.bridge.fitbit.worker;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.io.File;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.exceptions.BridgeSynapseException;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.fitbit.schema.TableSchema;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.synapse.SynapseHelper;

public class TableProcessorEdgeCasesTest {
    private static final String DATE_STRING = "2017-12-11";
    private static final String STUDY_ID = "test-study";
    private static final Study STUDY = new Study().identifier(STUDY_ID);
    private static final String SYNAPSE_TABLE_ID = "synapse-table";
    private static final String TABLE_ID = "my-table";

    private SynapseHelper mockSynapseHelper;
    private TableProcessor processor;
    private RequestContext ctx;

    @BeforeMethod
    public void setup() throws Exception {
        // Spy Table Processor. convertInMemoryTableToTsv() and verifySynapseTable() are tested elsewhere.
        processor = spy(new TableProcessor());
        doNothing().when(processor).convertInMemoryTableToTsv(any(), any());
        doReturn(SYNAPSE_TABLE_ID).when(processor).verifySynapseTable(any(), any());

        // Mock back-ends
        InMemoryFileHelper inMemoryFileHelper = new InMemoryFileHelper();
        mockSynapseHelper = mock(SynapseHelper.class);

        processor.setFileHelper(inMemoryFileHelper);
        processor.setSynapseHelper(mockSynapseHelper);

        // Make request context
        ctx = new RequestContext(DATE_STRING, STUDY, inMemoryFileHelper.createTempDir());
    }

    // branch coverage
    @Test
    public void emptyTable() throws Exception {
        // Make populated table with no rows
        TableSchema mockTableSchema = mock(TableSchema.class);
        PopulatedTable populatedTable = new PopulatedTable(TABLE_ID, mockTableSchema);

        // Execute
        processor.processTable(ctx, populatedTable);

        // Verify we did nothing.
        verify(processor, never()).convertInMemoryTableToTsv(any(), any());
        verify(processor, never()).verifySynapseTable(any(), any());
        verifyZeroInteractions(mockSynapseHelper);
    }

    // branch coverage
    @Test
    public void wrongNumberOfLinesProcessed() throws Exception {
        // Make populated table with 1 row
        Map<String, String> row = ImmutableMap.<String, String>builder()
                .put(Constants.COLUMN_HEALTH_CODE, "my-health-code")
                .put(Constants.COLUMN_CREATED_DATE, "2017-12-11")
                .build();

        TableSchema mockTableSchema = mock(TableSchema.class);
        PopulatedTable populatedTable = new PopulatedTable(TABLE_ID, mockTableSchema);
        populatedTable.getRowList().add(row);

        // Synapse Helper writes 2 lines.
        when(mockSynapseHelper.uploadTsvFileToTable(any(), any())).thenReturn(2L);

        // Execute
        try {
            processor.processTable(ctx, populatedTable);
            fail("expected exception");
        } catch (BridgeSynapseException ex) {
            assertEquals(ex.getMessage(), "Wrong number of lines processed importing to table=" +
                    SYNAPSE_TABLE_ID + ", expected=1, actual=2");
        }

        // Verify backends
        ArgumentCaptor<File> tsvFileCaptor = ArgumentCaptor.forClass(File.class);
        verify(processor).convertInMemoryTableToTsv(same(populatedTable), tsvFileCaptor.capture());
        File tsvFile = tsvFileCaptor.getValue();
        assertEquals(tsvFile.getName(), TABLE_ID + ".tsv");

        verify(processor).verifySynapseTable(ctx, populatedTable);

        verify(mockSynapseHelper).uploadTsvFileToTable(SYNAPSE_TABLE_ID, tsvFile);
    }
}
