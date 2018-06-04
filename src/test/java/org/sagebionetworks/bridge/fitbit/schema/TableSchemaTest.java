package org.sagebionetworks.bridge.fitbit.schema;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

public class TableSchemaTest {
    private static final String TABLE_KEY = "dummy-table-key";
    private static final ColumnSchema FOO_COLUMN_SCHEMA = new ColumnSchema.Builder().withColumnId("foo")
            .withColumnType(ColumnType.INTEGER).build();
    private static final ColumnSchema BAR_COLUMN_SCHEMA = new ColumnSchema.Builder().withColumnId("bar")
            .withColumnType(ColumnType.STRING).withMaxLength(250).build();
    private static final List<ColumnSchema> COLUMN_SCHEMA_LIST = ImmutableList.of(FOO_COLUMN_SCHEMA,
            BAR_COLUMN_SCHEMA);

    @Test
    public void success() {
        TableSchema tableSchema = new TableSchema.Builder().withTableKey(TABLE_KEY).withColumns(COLUMN_SCHEMA_LIST)
                .build();
        assertEquals(tableSchema.getTableKey(), TABLE_KEY);
        assertEquals(tableSchema.getColumns(), COLUMN_SCHEMA_LIST);

        Map<String, ColumnSchema> columnsById = tableSchema.getColumnsById();
        assertEquals(columnsById.size(), 2);
        assertEquals(columnsById.get(FOO_COLUMN_SCHEMA.getColumnId()), FOO_COLUMN_SCHEMA);
        assertEquals(columnsById.get(BAR_COLUMN_SCHEMA.getColumnId()), BAR_COLUMN_SCHEMA);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "tableKey must be specified")
    public void nullTableKey() {
        new TableSchema.Builder().withTableKey(null).withColumns(COLUMN_SCHEMA_LIST).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "tableKey must be specified")
    public void emptyTableKey() {
        new TableSchema.Builder().withTableKey("").withColumns(COLUMN_SCHEMA_LIST).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "tableKey must be specified")
    public void blankTableKey() {
        new TableSchema.Builder().withTableKey("   ").withColumns(COLUMN_SCHEMA_LIST).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "columns must be non-null and non-empty")
    public void nullColumns() {
        new TableSchema.Builder().withTableKey(TABLE_KEY).withColumns(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "columns must be non-null and non-empty")
    public void emptyColumns() {
        new TableSchema.Builder().withTableKey(TABLE_KEY).withColumns(ImmutableList.of()).build();
    }

    @Test
    public void deserialization() throws Exception {
        // Serialization of ColumnSchema is tested in ColumnSchemaTest. For this test, just use the serialized value of
        // a ColumnSchema so we don't have to worry about testing it twice.

        // Start with JSON
        String jsonText = "{\n" +
                "   \"tableKey\":\"" + TABLE_KEY + "\",\n" +
                "   \"columns\":" + DefaultObjectMapper.INSTANCE.writeValueAsString(COLUMN_SCHEMA_LIST) + "\n" +
                "}";

        // Convert to POJO
        TableSchema tableSchema = DefaultObjectMapper.INSTANCE.readValue(jsonText, TableSchema.class);
        assertEquals(tableSchema.getTableKey(), TABLE_KEY);
        assertEquals(tableSchema.getColumns(), COLUMN_SCHEMA_LIST);
        // Verify that columnsById exists, to make sure the class was constructed correctly. In-depth testing is done
        // elsewhere.
        assertNotNull(tableSchema.getColumnsById());

        // Convert back to JSON node
        JsonNode jsonNode = DefaultObjectMapper.INSTANCE.convertValue(tableSchema, JsonNode.class);
        assertEquals(jsonNode.size(), 2);
        assertEquals(jsonNode.get("tableKey").textValue(), TABLE_KEY);
        assertEquals(jsonNode.get("columns"), DefaultObjectMapper.INSTANCE.convertValue(COLUMN_SCHEMA_LIST,
                JsonNode.class));
    }

    @Test
    public void hashCodeEquals() {
        EqualsVerifier.forClass(TableSchema.class).verify();
    }
}
