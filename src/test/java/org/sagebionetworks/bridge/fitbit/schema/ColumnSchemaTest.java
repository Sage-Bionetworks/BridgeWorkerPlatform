package org.sagebionetworks.bridge.fitbit.schema;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

public class ColumnSchemaTest {
    private static final String COLUMN_ID = "dummy-column";

    @Test
    public void success() {
        ColumnSchema columnSchema = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
                .withColumnType(ColumnType.INTEGER).build();
        assertEquals(columnSchema.getColumnId(), COLUMN_ID);
        assertEquals(columnSchema.getColumnType(), ColumnType.INTEGER);
        assertNull(columnSchema.getMaxLength());
    }

    @Test
    public void withMaxLength() {
        ColumnSchema columnSchema = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
                .withColumnType(ColumnType.STRING).withMaxLength(250).build();
        assertEquals(columnSchema.getColumnId(), COLUMN_ID);
        assertEquals(columnSchema.getColumnType(), ColumnType.STRING);
        assertEquals(columnSchema.getMaxLength().intValue(), 250);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "columnId must be specified")
    public void nullColumnId() {
        new ColumnSchema.Builder().withColumnId(null).withColumnType(ColumnType.STRING).withMaxLength(250).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "columnId must be specified")
    public void emptyColumnId() {
        new ColumnSchema.Builder().withColumnId("").withColumnType(ColumnType.STRING).withMaxLength(250).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "columnId must be specified")
    public void blankColumnId() {
        new ColumnSchema.Builder().withColumnId("   ").withColumnType(ColumnType.STRING).withMaxLength(250).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "columnType must be specified")
    public void nullColumnType() {
        new ColumnSchema.Builder().withColumnId(COLUMN_ID).withColumnType(null).withMaxLength(250).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "maxLength must be specified for string types")
    public void stringTypeWithoutMaxLength() {
        new ColumnSchema.Builder().withColumnId(COLUMN_ID).withColumnType(ColumnType.STRING).withMaxLength(null)
                .build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "maxLength must be between 1 and 1000")
    public void negativeMaxLength() {
        new ColumnSchema.Builder().withColumnId(COLUMN_ID).withColumnType(ColumnType.STRING).withMaxLength(-1).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "maxLength must be between 1 and 1000")
    public void zeroMaxLength() {
        new ColumnSchema.Builder().withColumnId(COLUMN_ID).withColumnType(ColumnType.STRING).withMaxLength(0).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "maxLength must be between 1 and 1000")
    public void maxLengthTooLarge() {
        new ColumnSchema.Builder().withColumnId(COLUMN_ID).withColumnType(ColumnType.STRING).withMaxLength(1001)
                .build();
    }

    @Test
    public void jsonSerialization() throws Exception {
        // Start with JSON
        String jsonText = "{\n" +
                "   \"columnId\":\"" + COLUMN_ID + "\",\n" +
                "   \"columnType\":\"STRING\",\n" +
                "   \"maxLength\":250\n" +
                "}";

        // Convert to POJO
        ColumnSchema columnSchema = DefaultObjectMapper.INSTANCE.readValue(jsonText, ColumnSchema.class);
        assertEquals(columnSchema.getColumnId(), COLUMN_ID);
        assertEquals(columnSchema.getColumnType(), ColumnType.STRING);
        assertEquals(columnSchema.getMaxLength().intValue(), 250);

        // Convert back to JSON node
        JsonNode jsonNode = DefaultObjectMapper.INSTANCE.convertValue(columnSchema, JsonNode.class);
        assertEquals(jsonNode.size(), 3);
        assertEquals(jsonNode.get("columnId").textValue(), COLUMN_ID);
        assertEquals(jsonNode.get("columnType").textValue(), "STRING");
        assertEquals(jsonNode.get("maxLength").intValue(), 250);
    }

    @Test
    public void hashCodeEquals() {
        EqualsVerifier.forClass(ColumnSchema.class).verify();
    }
}
