package org.sagebionetworks.bridge.fitbit.worker;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.mockito.ArgumentCaptor;
import org.sagebionetworks.client.exceptions.SynapseNotFoundException;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.fitbit.schema.ColumnSchema;
import org.sagebionetworks.bridge.fitbit.schema.TableSchema;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.synapse.SynapseHelper;

@SuppressWarnings("unchecked")
public class TableProcessorTest {
    private static final String COLUMN_ID = "my-column";
    private static final int COLUMN_MAX_LENGTH = 48;
    private static final String DATE_STRING = "2017-12-11";
    private static final String HEALTH_CODE = "my-health-code";
    private static final String STUDY_ID = "test-study";
    private static final long SYNAPSE_DATA_ACCESS_TEAM_ID = 7777L;
    private static final long SYNAPSE_PRINCIPAL_ID = 1234567890L;
    private static final String SYNAPSE_PROJECT_ID = "my-synapse-project";
    private static final String SYNAPSE_TABLE_ID = "my-synapse-table";
    private static final String TABLE_ID = "my-table";
    private static final String TABLE_KEY = "table-key";

    private static final Study STUDY = new Study().identifier(STUDY_ID)
            .synapseDataAccessTeamId(SYNAPSE_DATA_ACCESS_TEAM_ID).synapseProjectId(SYNAPSE_PROJECT_ID);

    private static final TableSchema TABLE_SCHEMA;
    static {
        // Table has a single column
        ColumnSchema columnSchema = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
                .withColumnType(ColumnType.STRING).withMaxLength(COLUMN_MAX_LENGTH).build();

        TABLE_SCHEMA = new TableSchema.Builder().withTableKey(TABLE_KEY).withColumns(ImmutableList.of(columnSchema))
                .build();
    }

    private RequestContext ctx;
    private InMemoryFileHelper inMemoryFileHelper;
    private Table mockDdbTablesMap;
    private SynapseHelper mockSynapseHelper;
    private PopulatedTable populatedTable;
    private TableProcessor processor;
    private File tmpDir;
    private byte[] tsvBytes;

    @BeforeMethod
    public void setup() throws Exception {
        // Reset state, because TestNG doesn't always do so.
        tsvBytes = null;

        // Mock back-ends
        inMemoryFileHelper = new InMemoryFileHelper();
        mockDdbTablesMap = mock(Table.class);
        mockSynapseHelper = mock(SynapseHelper.class);

        // Mock SynapseHelper to capture the uploaded file.
        when(mockSynapseHelper.uploadTsvFileToTable(eq(SYNAPSE_TABLE_ID), any())).thenAnswer(invocation -> {
            // Captured uploaded file.
            File tsvFile = invocation.getArgumentAt(1, File.class);
            tsvBytes = inMemoryFileHelper.getBytes(tsvFile);

            // All tests in this class write 3 lines.
            return 3;
        });

        // Creating a Synapse table should return table ID. (We verify the args elsewhere.
        when(mockSynapseHelper.createTableWithColumnsAndAcls(any(), any(Set.class), any(Set.class), any(), any()))
                .thenReturn(SYNAPSE_TABLE_ID);

        // Set up Table Processor
        processor = new TableProcessor();
        processor.setFileHelper(inMemoryFileHelper);
        processor.setDdbTablesMap(mockDdbTablesMap);
        processor.setSynapseHelper(mockSynapseHelper);
        processor.setSynapsePrincipalId(SYNAPSE_PRINCIPAL_ID);

        // Make request context
        tmpDir = inMemoryFileHelper.createTempDir();
        ctx = new RequestContext(DATE_STRING, STUDY, tmpDir);

        // Make populated table
        populatedTable = new PopulatedTable(TABLE_ID, TABLE_SCHEMA);
        addRow("foo");
        addRow("bar");
        addRow("baz");
    }

    @Test
    public void tableExists() throws Exception {
        // Mock DDB and Synapse to already have the table.
        mockDdbWithTable();
        // Note: We don't actually care about the return value here, just that it doesn't throw.
        when(mockSynapseHelper.getTableWithRetry(SYNAPSE_TABLE_ID)).thenReturn(new TableEntity());

        // Execute and validate
        processor.processTable(ctx, populatedTable);
        validateTsv();
        validateCleanFileSystem();

        // Verify back-ends
        verify(mockSynapseHelper, never()).createTableWithColumnsAndAcls(any(), any(Set.class), any(Set.class), any(), any());
        verify(mockDdbTablesMap, never()).putItem(any(Item.class));

        ArgumentCaptor<List> columnModelListCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockSynapseHelper).safeUpdateTable(eq(SYNAPSE_TABLE_ID), columnModelListCaptor.capture(),
                eq(true));
        validateColumnModelList(columnModelListCaptor.getValue());
    }

    @Test
    public void tableDoesNotExists() throws Exception {
        // Mock DDB will return null by default.

        // Delegate test
        createTableTest();
    }

    @Test
    public void tableInDdbButNotInSynapse() throws Exception {
        // Mock DDB to return a table, but mock Synapse to throw.
        mockDdbWithTable();
        when(mockSynapseHelper.getTableWithRetry(SYNAPSE_TABLE_ID)).thenThrow(SynapseNotFoundException.class);

        // Delegate test
        createTableTest();
    }

    private void createTableTest() throws Exception {
        // Execute and validate
        processor.processTable(ctx, populatedTable);
        validateTsv();
        validateCleanFileSystem();

        // Verify back-ends
        verify(mockSynapseHelper, never()).safeUpdateTable(any(), any(), anyBoolean());

        ArgumentCaptor<List> columnModelListCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockSynapseHelper).createTableWithColumnsAndAcls(columnModelListCaptor.capture(),
                eq(ImmutableSet.of(SYNAPSE_DATA_ACCESS_TEAM_ID)), eq(ImmutableSet.of(SYNAPSE_PRINCIPAL_ID)),
                eq(SYNAPSE_PROJECT_ID), eq(TABLE_ID));
        validateColumnModelList(columnModelListCaptor.getValue());

        ArgumentCaptor<Item> itemCaptor = ArgumentCaptor.forClass(Item.class);
        verify(mockDdbTablesMap).putItem(itemCaptor.capture());

        Item item = itemCaptor.getValue();
        assertEquals(item.getString(TableProcessor.DDB_KEY_STUDY_ID), STUDY_ID);
        assertEquals(item.getString(TableProcessor.DDB_KEY_TABLE_ID), TABLE_ID);
        assertEquals(item.getString(TableProcessor.DDB_KEY_SYNAPSE_TABLE_ID), SYNAPSE_TABLE_ID);
    }

    private void addRow(String value) {
        Map<String, String> row = ImmutableMap.<String, String>builder().put(Constants.COLUMN_HEALTH_CODE, HEALTH_CODE)
                .put(Constants.COLUMN_CREATED_DATE, DATE_STRING).put(COLUMN_ID, value).build();
        populatedTable.getRowList().add(row);
    }

    private void mockDdbWithTable() {
        Item tableMapItem = new Item().withString(TableProcessor.DDB_KEY_STUDY_ID, STUDY_ID)
                .withString(TableProcessor.DDB_KEY_TABLE_ID, TABLE_ID)
                .withString(TableProcessor.DDB_KEY_SYNAPSE_TABLE_ID, SYNAPSE_TABLE_ID);
        when(mockDdbTablesMap.getItem(TableProcessor.DDB_KEY_STUDY_ID, STUDY_ID, TableProcessor.DDB_KEY_TABLE_ID,
                TABLE_ID)).thenReturn(tableMapItem);
    }

    private void validateTsv() {
        String tsvText = new String(tsvBytes);
        String[] tsvLines = tsvText.split("\n");
        assertEquals(tsvLines[0], Constants.COLUMN_HEALTH_CODE + '\t' + Constants.COLUMN_CREATED_DATE + '\t' +
                COLUMN_ID);
        assertEquals(tsvLines[1], HEALTH_CODE + '\t' + DATE_STRING + '\t' + "foo");
        assertEquals(tsvLines[2], HEALTH_CODE + '\t' + DATE_STRING + '\t' + "bar");
        assertEquals(tsvLines[3], HEALTH_CODE + '\t' + DATE_STRING + '\t' + "baz");
    }

    private void validateCleanFileSystem() {
        // TableProcessor creates and deletes the tsvFile. We create and delete the tmpDir. Delete the tmpDir and
        // verify that the file system is clean.
        inMemoryFileHelper.deleteDir(tmpDir);
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    private static void validateColumnModelList(List<ColumnModel> columnModelList) {
        assertEquals(columnModelList.size(), 3);

        assertEquals(columnModelList.get(0).getName(), Constants.COLUMN_HEALTH_CODE);
        assertEquals(columnModelList.get(0).getColumnType(), ColumnType.STRING);
        assertEquals(columnModelList.get(0).getMaximumSize().intValue(), 36);

        assertEquals(columnModelList.get(1).getName(), Constants.COLUMN_CREATED_DATE);
        assertEquals(columnModelList.get(1).getColumnType(), ColumnType.STRING);
        assertEquals(columnModelList.get(1).getMaximumSize().intValue(), 10);

        assertEquals(columnModelList.get(2).getName(), COLUMN_ID);
        assertEquals(columnModelList.get(2).getColumnType(), ColumnType.STRING);
        assertEquals(columnModelList.get(2).getMaximumSize().intValue(), COLUMN_MAX_LENGTH);
    }
}
