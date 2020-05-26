package org.sagebionetworks.bridge.fitbit.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.fitbit.schema.ColumnSchema;
import org.sagebionetworks.bridge.fitbit.schema.TableSchema;
import org.sagebionetworks.bridge.fitbit.worker.PopulatedTable;
import org.sagebionetworks.bridge.rest.model.OAuthProvider;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.workerPlatform.util.Constants;

public class UtilsTest {
    private static final long DATA_ACCESS_TEAM_ID = 1234L;
    private static final String PROJECT_ID = "my-project";
    private static final String TABLE_KEY = "my-table-key";
    private static final String TABLE_ID = "my-table-id";

    @Test
    public void getAllColumnsForTable() {
        // Make populated table with table schema.
        ColumnSchema myColumnSchema = new ColumnSchema.Builder().withColumnId("my-column")
                .withColumnType(ColumnType.INTEGER).build();
        TableSchema myTableSchema = new TableSchema.Builder().withTableKey(TABLE_KEY)
                .withColumns(ImmutableList.of(myColumnSchema)).build();
        PopulatedTable populatedTable = new PopulatedTable(TABLE_ID, myTableSchema);

        // Execute and validate.
        List<ColumnSchema> allColumnList = Utils.getAllColumnsForTable(populatedTable);
        assertEquals(allColumnList.size(), 4);
        assertEquals(allColumnList.get(0), Utils.COMMON_COLUMN_LIST.get(0));
        assertEquals(allColumnList.get(1), Utils.COMMON_COLUMN_LIST.get(1));
        assertEquals(allColumnList.get(2), Utils.COMMON_COLUMN_LIST.get(2));
        assertEquals(allColumnList.get(3), myColumnSchema);
    }

    @Test
    public void getColumnModelForSchemaStringType() {
        ColumnSchema columnSchema = new ColumnSchema.Builder().withColumnId("my-column")
                .withColumnType(ColumnType.STRING).withMaxLength(42).build();
        ColumnModel columnModel = Utils.getColumnModelForSchema(columnSchema);
        assertEquals(columnModel.getName(), "my-column");
        assertEquals(columnModel.getColumnType(), ColumnType.STRING);
        assertEquals(columnModel.getMaximumSize().intValue(), 42);
    }

    @Test
    public void getColumnModelForSchemaIntType() {
        ColumnSchema columnSchema = new ColumnSchema.Builder().withColumnId("my-column")
                .withColumnType(ColumnType.INTEGER).build();
        ColumnModel columnModel = Utils.getColumnModelForSchema(columnSchema);
        assertEquals(columnModel.getName(), "my-column");
        assertEquals(columnModel.getColumnType(), ColumnType.INTEGER);
        assertNull(columnModel.getMaximumSize());
    }

    @Test
    public void isConfigured() {
        assertTrue(Utils.isStudyConfigured(makeConfiguredStudy()));
    }

    @Test
    public void noSynapseProjectId() {
        Study study = makeConfiguredStudy().synapseProjectId(null);
        assertFalse(Utils.isStudyConfigured(study));
    }

    @Test
    public void noDataAccessTeam() {
        Study study = makeConfiguredStudy().synapseDataAccessTeamId(null);
        assertFalse(Utils.isStudyConfigured(study));
    }

    @Test
    public void noOAuthProviders() {
        Study study = makeConfiguredStudy().oAuthProviders(null);
        assertFalse(Utils.isStudyConfigured(study));
    }

    @Test
    public void oAuthProvidersDontContainFitBit() {
        Study study = makeConfiguredStudy().oAuthProviders(ImmutableMap.of());
        assertFalse(Utils.isStudyConfigured(study));
    }

    private static Study makeConfiguredStudy() {
        return new Study().synapseProjectId(PROJECT_ID).synapseDataAccessTeamId(DATA_ACCESS_TEAM_ID)
                .putOAuthProvidersItem(Constants.FITBIT_VENDOR_ID, new OAuthProvider());
    }

    @Test
    public void writeRowToTsv() throws Exception {
        // Write
        String output;
        try (StringWriter stringWriter = new StringWriter();
                PrintWriter printWriter = new PrintWriter(stringWriter)) {
            Utils.writeRowToTsv(printWriter, ImmutableList.of("foo", "bar", "baz"));
            Utils.writeRowToTsv(printWriter, ImmutableList.of("qwerty", "asdf", "jkl;"));
            Utils.writeRowToTsv(printWriter, ImmutableList.of("AAA", "BBB", "CCC"));
            output = stringWriter.toString();
        }

        // Verify result
        String[] lines = output.split("\n");
        assertEquals(lines.length, 3);
        assertEquals(lines[0], "foo\tbar\tbaz");
        assertEquals(lines[1], "qwerty\tasdf\tjkl;");
        assertEquals(lines[2], "AAA\tBBB\tCCC");
    }
}
