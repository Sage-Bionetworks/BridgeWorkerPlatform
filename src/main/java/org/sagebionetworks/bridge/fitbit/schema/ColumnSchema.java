package org.sagebionetworks.bridge.fitbit.schema;

import java.util.Objects;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.commons.lang3.StringUtils;
import org.sagebionetworks.repo.model.table.ColumnType;

/**
 * Represents a column schema, a list of attributes in FitBit data and the Synapse table column types to serialize them
 * into.
 */
@JsonDeserialize(builder = ColumnSchema.Builder.class)
public class ColumnSchema {
    private final String columnId;
    private final ColumnType columnType;
    private final Integer maxLength;

    /** Private constructor. To construct, use Builder. */
    private ColumnSchema(String columnId, ColumnType columnType, Integer maxLength) {
        this.columnId = columnId;
        this.columnType = columnType;
        this.maxLength = maxLength;
    }

    /** Unique identifier (name) for the column. */
    public String getColumnId() {
        return columnId;
    }

    /** Synapse table column type. */
    public ColumnType getColumnType() {
        return columnType;
    }

    /** For string types only, what is the max length. Cannot be larger than 1000. */
    public Integer getMaxLength() {
        return maxLength;
    }

    /** {@inheritDoc} */
    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnSchema)) {
            return false;
        }
        ColumnSchema that = (ColumnSchema) o;
        return Objects.equals(columnId, that.columnId) &&
                columnType == that.columnType &&
                Objects.equals(maxLength, that.maxLength);
    }

    /** {@inheritDoc} */
    @Override
    public final int hashCode() {
        return Objects.hash(columnId, columnType, maxLength);
    }

    /** Builder */
    public static class Builder {
        private String columnId;
        private ColumnType columnType;
        private Integer maxLength;

        /** @see ColumnSchema#getColumnId */
        public Builder withColumnId(String columnId) {
            this.columnId = columnId;
            return this;
        }

        /** @see ColumnSchema#getColumnType */
        public Builder withColumnType(ColumnType columnType) {
            this.columnType = columnType;
            return this;
        }

        /** @see ColumnSchema#getMaxLength */
        public Builder withMaxLength(Integer maxLength) {
            this.maxLength = maxLength;
            return this;
        }

        /** Builds a ColumnSchema */
        public ColumnSchema build() {
            // Column ID must be specified
            if (StringUtils.isBlank(columnId)) {
                throw new IllegalStateException("columnId must be specified");
            }

            // Column Type must be specified
            if (columnType == null) {
                throw new IllegalStateException("columnType must be specified");
            }

            // If Column Type is String, maxLength must be specified
            if (columnType == ColumnType.STRING && maxLength == null) {
                throw new IllegalStateException("maxLength must be specified for string types");
            }

            // If maxLength is specified, it must be between 1 and 1000 (inclusive)
            if (maxLength != null && (maxLength < 1 || maxLength > 1000)) {
                throw new IllegalStateException("maxLength must be between 1 and 1000");
            }

            return new ColumnSchema(columnId, columnType, maxLength);
        }
    }
}
