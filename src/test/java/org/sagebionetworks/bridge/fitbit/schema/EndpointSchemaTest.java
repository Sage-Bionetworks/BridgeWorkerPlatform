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
    private static final String SCOPE_NAME = "DUMMY_SCOPE";
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
        EndpointSchema endpointSchema = makeValidBuilder().build();
        assertEquals(endpointSchema.getEndpointId(), ENDPOINT_ID);
        assertTrue(endpointSchema.getIgnoredKeys().isEmpty());
        assertEquals(endpointSchema.getScopeName(), SCOPE_NAME);
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
        EndpointSchema endpointSchema = makeValidBuilder().withIgnoredKeys(IGNORED_KEYS)
                .withUrlParameters(URL_PARAMETERS).build();
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
        makeValidBuilder().withEndpointId(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "endpointId must be specified")
    public void emptyEndpointId() {
        makeValidBuilder().withEndpointId("").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "endpointId must be specified")
    public void blankEndpointId() {
        makeValidBuilder().withEndpointId("   ").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "scopeName must be specified")
    public void nullScopeName() {
        makeValidBuilder().withScopeName(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "scopeName must be specified")
    public void emptyScopeName() {
        makeValidBuilder().withScopeName("").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "scopeName must be specified")
    public void blankScopeName() {
        makeValidBuilder().withScopeName("   ").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "url must be specified")
    public void nullUrl() {
        makeValidBuilder().withUrl(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "url must be specified")
    public void emptyUrl() {
        makeValidBuilder().withUrl("").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "url must be specified")
    public void blankUrl() {
        makeValidBuilder().withUrl("   ").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "tables must be non-null and non-empty")
    public void nullTables() {
        makeValidBuilder().withTables(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "tables must be non-null and non-empty")
    public void emptyTables() {
        makeValidBuilder().withTables(ImmutableList.of()).build();
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
                "   \"scopeName\":\"" + SCOPE_NAME + "\",\n" +
                "   \"url\":\"" + URL + "\",\n" +
                "   \"urlParameters\":[\"DATE\", \"USER_ID\"],\n" +
                "   \"tables\":" + DefaultObjectMapper.INSTANCE.writeValueAsString(TABLE_SCHEMA_LIST) + "\n" +
                "}";

        // Convert to POJO
        EndpointSchema endpointSchema = DefaultObjectMapper.INSTANCE.readValue(jsonText, EndpointSchema.class);
        assertEquals(endpointSchema.getEndpointId(), ENDPOINT_ID);
        assertEquals(endpointSchema.getIgnoredKeys(), IGNORED_KEYS);
        assertEquals(endpointSchema.getScopeName(), SCOPE_NAME);
        assertEquals(endpointSchema.getUrl(), URL);
        assertEquals(endpointSchema.getUrlParameters(), URL_PARAMETERS);
        assertEquals(endpointSchema.getTables(), TABLE_SCHEMA_LIST);
        // tablesByKey is already tested above. Just test that it exists.
        assertNotNull(endpointSchema.getTablesByKey());
    }

    private static EndpointSchema.Builder makeValidBuilder() {
        return new EndpointSchema.Builder().withEndpointId(ENDPOINT_ID).withUrl(URL).withScopeName(SCOPE_NAME)
                .withTables(TABLE_SCHEMA_LIST);
    }
}
