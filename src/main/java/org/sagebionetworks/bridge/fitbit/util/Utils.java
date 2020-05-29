package org.sagebionetworks.bridge.fitbit.util;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.ColumnType;

import org.sagebionetworks.bridge.fitbit.schema.ColumnSchema;
import org.sagebionetworks.bridge.fitbit.worker.PopulatedTable;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.workerPlatform.util.Constants;

/** Utility functions */
public class Utils {
    private static final Joiner JOINER_COLUMN_JOINER = Joiner.on('\t').useForNull("");

    // Every table has a healthCode (guid) and createdOn (YYYY-MM-DD) column.
    // Visible for testing
    static final List<ColumnSchema> COMMON_COLUMN_LIST = ImmutableList.of(
            new ColumnSchema.Builder().withColumnId(Constants.COLUMN_HEALTH_CODE).withColumnType(ColumnType.STRING)
                    .withMaxLength(36).build(),
            new ColumnSchema.Builder().withColumnId(Constants.COLUMN_CREATED_DATE).withColumnType(ColumnType.STRING)
                    .withMaxLength(10).build(),
            new ColumnSchema.Builder().withColumnId(Constants.COLUMN_RAW_DATA).withColumnType(ColumnType.FILEHANDLEID)
                    .build());

    /**
     * Helper method which merges the common column list with the table-specific column schemas and returns the full
     * list of table columns.
     */
    public static List<ColumnSchema> getAllColumnsForTable(PopulatedTable table) {
        // Combine common columns with table-specific columns.
        List<ColumnSchema> allColumnList = new ArrayList<>();
        allColumnList.addAll(COMMON_COLUMN_LIST);
        allColumnList.addAll(table.getTableSchema().getColumns());
        return allColumnList;
    }

    /** Helper method which converts a ColumnSchema to a Synapse ColumnModel. */
    public static ColumnModel getColumnModelForSchema(ColumnSchema schema) {
        // Column ID in the schema is column name in the model.
        ColumnModel columnModel = new ColumnModel();
        columnModel.setName(schema.getColumnId());
        columnModel.setColumnType(schema.getColumnType());
        if (schema.getMaxLength() != null) {
            columnModel.setMaximumSize(schema.getMaxLength().longValue());
        }
        return columnModel;
    }

    /**
     * Returns true if the app is configured for FitBit data export. This means that the app is configured to
     * export to Synapse (has the synapseProjectId and synapseDataAccessTeamId properties) and is configured for FitBit
     * OAuth (has "fitbit" in its oAuthProviders).
     */
    public static boolean isAppConfigured(App app) {
        return app.getSynapseProjectId() != null
                && app.getSynapseDataAccessTeamId() != null
                && app.getOAuthProviders() != null
                && app.getOAuthProviders().containsKey(Constants.FITBIT_VENDOR_ID);
    }

    /** Helper method, which formats and writes a row of values (represented as a String List) to the given Writer. */
    public static void writeRowToTsv(PrintWriter tsvWriter, List<String> rowValueList) {
        tsvWriter.println(JOINER_COLUMN_JOINER.join(rowValueList));
    }
}
