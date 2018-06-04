package org.sagebionetworks.bridge.fitbit.schema;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

public class EndpointSchemaTest {
    private static final String ENDPOINT_ID = "test-endpoint";
    private static final Set<String> IGNORED_KEYS = ImmutableSet.of("ignore-asdf", "ignore-jkl;");
    private static final String URL = "http://example.com/";
    private static final List<UrlParameterType> URL_PARAMETERS = ImmutableList.of(UrlParameterType.DATE,
            UrlParameterType.USER_ID);

    private static final ColumnSchema FOO_COLUMN_SCHEMA = new ColumnSchema.Builder().withColumnId("foo-column")
            .withColumnType(ColumnType.INTEGER).build();
    private static final TableSchema FOO_TABLE_SCHEMA = new TableSchema.Builder().withTableKey("foo-table")
            .withColumns(ImmutableList.of(FOO_COLUMN_SCHEMA)).build();

    private static final ColumnSchema BAR_COLUMN_SCHEMA = new ColumnSchema.Builder().withColumnId("bar-column")
            .withColumnType(ColumnType.INTEGER).build();
    private static final TableSchema BAR_TABLE_SCHEMA = new TableSchema.Builder().withTableKey("bar-table")
            .withColumns(ImmutableList.of(BAR_COLUMN_SCHEMA)).build();

    private static final List<TableSchema> TABLE_SCHEMA_LIST = ImmutableList.of(FOO_TABLE_SCHEMA, BAR_TABLE_SCHEMA);

    @Test
    public void success() {
        EndpointSchema endpointSchema = new EndpointSchema.Builder().withEndpointId(ENDPOINT_ID).withUrl(URL)
                .withTables(TABLE_SCHEMA_LIST).build();
        assertEquals(endpointSchema.getEndpointId(), ENDPOINT_ID);
        assertTrue(endpointSchema.getIgnoredKeys().isEmpty());
        assertEquals(endpointSchema.getUrl(), URL);
        assertTrue(endpointSchema.getUrlParameters().isEmpty());
        assertEquals(endpointSchema.getTables(), TABLE_SCHEMA_LIST);

        Map<String, TableSchema> tablesByKey = endpointSchema.getTablesByKey();
        assertEquals(tablesByKey.size(), 2);
        assertEquals(tablesByKey.get(FOO_TABLE_SCHEMA.getTableKey()), FOO_TABLE_SCHEMA);
        assertEquals(tablesByKey.get(BAR_TABLE_SCHEMA.getTableKey()), BAR_TABLE_SCHEMA);
    }

    @Test
    public void optionalParams() {
        EndpointSchema endpointSchema = new EndpointSchema.Builder().withEndpointId(ENDPOINT_ID)
                .withIgnoredKeys(IGNORED_KEYS).withUrl(URL).withUrlParameters(URL_PARAMETERS)
                .withTables(TABLE_SCHEMA_LIST).build();
        assertEquals(endpointSchema.getEndpointId(), ENDPOINT_ID);
        assertEquals(endpointSchema.getIgnoredKeys(), IGNORED_KEYS);
        assertEquals(endpointSchema.getUrl(), URL);
        assertEquals(endpointSchema.getUrlParameters(), URL_PARAMETERS);
        assertEquals(endpointSchema.getTables(), TABLE_SCHEMA_LIST);
        // tablesByKey is already tested above. Just test that it exists.
        assertNotNull(endpointSchema.getTablesByKey());
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "endpointId must be specified")
    public void nullEndpointId() {
        new EndpointSchema.Builder().withEndpointId(null).withUrl(URL).withTables(TABLE_SCHEMA_LIST).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "endpointId must be specified")
    public void emptyEndpointId() {
        new EndpointSchema.Builder().withEndpointId("").withUrl(URL).withTables(TABLE_SCHEMA_LIST).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "endpointId must be specified")
    public void blankEndpointId() {
        new EndpointSchema.Builder().withEndpointId("   ").withUrl(URL).withTables(TABLE_SCHEMA_LIST).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "url must be specified")
    public void nullUrl() {
        new EndpointSchema.Builder().withEndpointId(ENDPOINT_ID).withUrl(null).withTables(TABLE_SCHEMA_LIST).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "url must be specified")
    public void emptyUrl() {
        new EndpointSchema.Builder().withEndpointId(ENDPOINT_ID).withUrl("").withTables(TABLE_SCHEMA_LIST).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "url must be specified")
    public void blankUrl() {
        new EndpointSchema.Builder().withEndpointId(ENDPOINT_ID).withUrl("   ").withTables(TABLE_SCHEMA_LIST).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "tables must be non-null and non-empty")
    public void nullTables() {
        new EndpointSchema.Builder().withEndpointId(ENDPOINT_ID).withUrl(URL).withTables(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "tables must be non-null and non-empty")
    public void emptyTables() {
        new EndpointSchema.Builder().withEndpointId(ENDPOINT_ID).withUrl(URL).withTables(ImmutableList.of()).build();
    }

    @Test
    public void deserialization() throws Exception {
        // We only ever deserialize, never serialize.
        // Serialization of TableSchema is tested in TableSchemaTest. For this test, just use the serialized value of
        // a TableSchema so we don't have to worry about testing it twice.

        // Start with JSON
        String jsonText = "{\n" +
                "   \"endpointId\":\"" + ENDPOINT_ID + "\",\n" +
                "   \"ignoredKeys\":[\"ignore-asdf\", \"ignore-jkl;\"],\n" +
                "   \"url\":\"" + URL + "\",\n" +
                "   \"urlParameters\":[\"DATE\", \"USER_ID\"],\n" +
                "   \"tables\":" + DefaultObjectMapper.INSTANCE.writeValueAsString(TABLE_SCHEMA_LIST) + "\n" +
                "}";

        // Convert to POJO
        EndpointSchema endpointSchema = DefaultObjectMapper.INSTANCE.readValue(jsonText, EndpointSchema.class);
        assertEquals(endpointSchema.getEndpointId(), ENDPOINT_ID);
        assertEquals(endpointSchema.getIgnoredKeys(), IGNORED_KEYS);
        assertEquals(endpointSchema.getUrl(), URL);
        assertEquals(endpointSchema.getUrlParameters(), URL_PARAMETERS);
        assertEquals(endpointSchema.getTables(), TABLE_SCHEMA_LIST);
        // tablesByKey is already tested above. Just test that it exists.
        assertNotNull(endpointSchema.getTablesByKey());
    }
}
