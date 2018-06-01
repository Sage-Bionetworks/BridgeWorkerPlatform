package org.sagebionetworks.bridge.fitbit.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.sagebionetworks.bridge.fitbit.schema.TableSchema;

/**
 * Represents a particular table for a particular job run. This keeps track of the state of this particular table as we
 * call the FitBit Web API and download data.
 */
public class PopulatedTable {
    // Instance invariants
    private final String tableId;
    private final TableSchema tableSchema;

    // Instance state tracking
    private final List<Map<String, String>> rowList = new ArrayList<>();

    /**
     * Constructs a Populated Table
     *
     * @param tableId
     *         table ID (table name in Synapse)
     * @param tableSchema
     *         table schema
     */
    public PopulatedTable(String tableId, TableSchema tableSchema) {
        this.tableId = tableId;
        this.tableSchema = tableSchema;
    }

    /** Table ID, used to uniquely identify a table withiin a study. Also used as the table name in Synapse. */
    public String getTableId() {
        return tableId;
    }

    /** Table Schema, used to determine table columns. */
    public TableSchema getTableSchema() {
        return tableSchema;
    }

    /** List of table rows. Each row is represented as a map from column name to column value. */
    public List<Map<String, String>> getRowList() {
        return rowList;
    }
}
