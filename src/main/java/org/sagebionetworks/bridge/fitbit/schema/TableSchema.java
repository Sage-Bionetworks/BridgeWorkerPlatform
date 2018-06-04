package org.sagebionetworks.bridge.fitbit.schema;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

/**
 * Represents a table schema. This includes a top-level key in the FitBit API JSON response and the columns it
 * contains.
 */
@JsonDeserialize(builder = TableSchema.Builder.class)
public class TableSchema {
    private final String tableKey;
    private final List<ColumnSchema> columns;
    private transient final Map<String, ColumnSchema> columnsById;

    /** Private constructor. To construct, use Builder. */
    private TableSchema(String tableKey, List<ColumnSchema> columns) {
        this.tableKey = tableKey;
        this.columns = columns;
        this.columnsById = Maps.uniqueIndex(columns, ColumnSchema::getColumnId);
    }

    /**
     * This is the top-level key in the FitBit API JSON response that this table should be parsed from. It's also used
     * to derive the Synapse table name.
     */
    public String getTableKey() {
        return tableKey;
    }

    /** List of columns in the table. */
    public List<ColumnSchema> getColumns() {
        return columns;
    }

    /** Map of columns in the table, keyed by column ID (name). */
    @JsonIgnore
    public Map<String, ColumnSchema> getColumnsById() {
        return columnsById;
    }

    /** {@inheritDoc} */
    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableSchema)) {
            return false;
        }
        TableSchema that = (TableSchema) o;
        return Objects.equals(tableKey, that.tableKey) &&
                Objects.equals(columns, that.columns);
    }

    /** {@inheritDoc} */
    @Override
    public final int hashCode() {
        return Objects.hash(tableKey, columns);
    }

    /** Builder */
    public static class Builder {
        private String tableKey;
        private List<ColumnSchema> columns;

        /** @see TableSchema#getTableKey */
        public Builder withTableKey(String tableKey) {
            this.tableKey = tableKey;
            return this;
        }

        /** @see TableSchema#getColumns */
        public Builder withColumns(List<ColumnSchema> columns) {
            this.columns = columns;
            return this;
        }

        /** Builds a TableSchema */
        public TableSchema build() {
            // Params must be non-null and non-empty.
            if (StringUtils.isBlank(tableKey)) {
                throw new IllegalStateException("tableKey must be specified");
            }
            if (columns == null || columns.isEmpty()) {
                throw new IllegalStateException("columns must be non-null and non-empty");
            }

            return new TableSchema(tableKey, ImmutableList.copyOf(columns));
        }
    }
}
