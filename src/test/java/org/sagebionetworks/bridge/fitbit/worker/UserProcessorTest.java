package org.sagebionetworks.bridge.fitbit.worker;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.http.client.HttpResponseException;
import org.mockito.ArgumentCaptor;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.table.ColumnType;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.FitBitUser;
import org.sagebionetworks.bridge.fitbit.schema.ColumnSchema;
import org.sagebionetworks.bridge.fitbit.schema.EndpointSchema;
import org.sagebionetworks.bridge.fitbit.schema.TableSchema;
import org.sagebionetworks.bridge.fitbit.schema.UrlParameterType;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.OAuthAccessToken;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.workerPlatform.util.Constants;

public class UserProcessorTest {
    private static final String COLUMN_ID = "my-column";
    private static final String DATE_STRING = "2017-12-12";
    private static final String ENDPOINT_ID = "my-endpoint";
    private static final String FILEHANDLE_ID = "my-file-handle";
    private static final String IGNORED_KEY = "ignored-key";
    private static final String SCOPE_NAME = "SCOPE_NAME";
    private static final String TABLE_KEY = "table-key";
    private static final String URL = "http://example.com/users/my-user/date/2017-12-12";
    private static final String URL_PATTERN = "http://example.com/users/%s/date/%s";

    private static final ColumnSchema BOOLEAN_COLUMN = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
            .withColumnType(ColumnType.BOOLEAN).build();
    private static final ColumnSchema DATE_COLUMN = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
            .withColumnType(ColumnType.DATE).build();
    private static final ColumnSchema DOUBLE_COLUMN = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
            .withColumnType(ColumnType.DOUBLE).build();
    private static final ColumnSchema FILEHANDLE_COLUMN = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
            .withColumnType(ColumnType.FILEHANDLEID).build();
    private static final ColumnSchema INTEGER_COLUMN = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
            .withColumnType(ColumnType.INTEGER).build();
    private static final ColumnSchema LARGETEXT_COLUMN = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
            .withColumnType(ColumnType.LARGETEXT).build();
    private static final ColumnSchema STRING_COLUMN = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
            .withColumnType(ColumnType.STRING).withMaxLength(10).build();
    private static final ColumnSchema UNSUPPORTED_COLUMN = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
            .withColumnType(ColumnType.LINK).build();

    private static final String STUDY_ID = "my-study";
    private static final Study STUDY = new Study().identifier(STUDY_ID);

    private static final String TABLE_ID = ENDPOINT_ID + '.' + TABLE_KEY;

    private static final String ACCESS_TOKEN = "my-access-token";
    private static final String HEALTH_CODE = "my-health-code";
    private static final String USER_ID = "my-user";

    private static final TableSchema TABLE_SCHEMA;
    private static final EndpointSchema ENDPOINT_SCHEMA;
    static {
        ColumnSchema columnSchema = new ColumnSchema.Builder().withColumnId(COLUMN_ID)
                .withColumnType(ColumnType.STRING).withMaxLength(128).build();
        TABLE_SCHEMA = new TableSchema.Builder().withTableKey(TABLE_KEY)
                .withColumns(ImmutableList.of(columnSchema)).build();
        ENDPOINT_SCHEMA = new EndpointSchema.Builder().withEndpointId(ENDPOINT_ID)
                .withIgnoredKeys(ImmutableSet.of(IGNORED_KEY)).withScopeName(SCOPE_NAME).withUrl(URL_PATTERN)
                .withUrlParameters(ImmutableList.of(UrlParameterType.USER_ID, UrlParameterType.DATE))
                .withTables(ImmutableList.of(TABLE_SCHEMA)).build();
    }

    private static FitBitUser user;

    private RequestContext ctx;
    private InMemoryFileHelper inMemoryFileHelper;
    private List<byte[]> uploadedFileBytesList;
    private String mockHttpResponse;
    private SynapseHelper mockSynapseHelper;
    int numFilesUploaded;
    private UserProcessor processor;

    @BeforeClass
    public static void beforeClass() {
        // Mock OAuth token. This is read-only, so it's easier to just mock it instead of using Reflection.
        OAuthAccessToken oauthToken = mock(OAuthAccessToken.class);
        when(oauthToken.getAccessToken()).thenReturn(ACCESS_TOKEN);
        when(oauthToken.getProviderUserId()).thenReturn(USER_ID);
        when(oauthToken.getScopes()).thenReturn(ImmutableList.of(SCOPE_NAME));

        user = new FitBitUser.Builder().withHealthCode(HEALTH_CODE).withToken(oauthToken).build();
    }

    @BeforeMethod
    public void setup() throws Exception {
        // Reset test params, because sometimes TestNG doesn't.
        mockHttpResponse = null;
        numFilesUploaded = 0;
        uploadedFileBytesList = new ArrayList<>();

        // Create in-memory file helper with temp dir.
        inMemoryFileHelper = new InMemoryFileHelper();
        File tempDir = inMemoryFileHelper.createTempDir();

        // Mock Synapse Helper. We need to capture the file bytes while it's being uploaded, because we delete the file
        // immediately afterwards.
        mockSynapseHelper = mock(SynapseHelper.class);
        when(mockSynapseHelper.createFileHandleWithRetry(any())).thenAnswer(invocation -> {
            // Save file bytes
            File uploadedFile = invocation.getArgumentAt(0, File.class);
            uploadedFileBytesList.add(inMemoryFileHelper.getBytes(uploadedFile));

            // Mock and return file handle
            FileHandle mockFileHandle = mock(FileHandle.class);
            when(mockFileHandle.getId()).thenReturn(FILEHANDLE_ID + numFilesUploaded);
            numFilesUploaded++;
            return mockFileHandle;
        });

        // Spy processor so we can mock out the rest call.
        processor = spy(new UserProcessor());
        processor.setFileHelper(inMemoryFileHelper);
        processor.setSynapseHelper(mockSynapseHelper);

        // Use a doAnswer(), so the tests can specify mockHttpResponse. The tests will also use verify() to validate
        // input args.
        doAnswer(invocation -> mockHttpResponse).when(processor).makeHttpRequest(any(), any());

        // Make request context.
        ctx = new RequestContext(DATE_STRING, STUDY, tempDir);
    }

    @Test
    public void normalCaseObjectTable() throws Exception {
        // Make HTTP response.
        mockHttpResponse = "{\n" +
                "   \"" + TABLE_KEY + "\":{\n" +
                "       \"" + COLUMN_ID + "\":\"Just one value\"\n" +
                "   }\n" +
                "}";

        // Execute and validate
        processor.processEndpointForUser(ctx, user, ENDPOINT_SCHEMA);

        List<Map<String, String>> rowList = validatePopulatedTablesById();
        assertEquals(rowList.size(), 1);
        validateRow(rowList.get(0), "Just one value", 0);
        verifyFileHelperClean();

        verify(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        verify(processor, never()).warnWrapper(any());
    }

    @Test
    public void normalCaseArrayTable() throws Exception {
        // Make HTTP response.
        mockHttpResponse = "{\n" +
                "   \"" + TABLE_KEY + "\":[\n" +
                "       {\"" + COLUMN_ID + "\":\"foo\"},\n" +
                "       {\"" + COLUMN_ID + "\":\"bar\"},\n" +
                "       {\"" + COLUMN_ID + "\":\"baz\"}\n" +
                "   ]\n" +
                "}";

        // Execute and validate
        processor.processEndpointForUser(ctx, user, ENDPOINT_SCHEMA);

        List<Map<String, String>> rowList = validatePopulatedTablesById();
        assertEquals(rowList.size(), 3);
        validateRow(rowList.get(0), "foo", 0);
        validateRow(rowList.get(1), "bar", 1);
        validateRow(rowList.get(2), "baz", 2);
        verifyFileHelperClean();

        verify(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        verify(processor, never()).warnWrapper(any());
    }

    @Test
    public void normalCaseContextAlreadyHasTable() throws Exception {
        // Set up context with previous user's data.
        Map<String, String> previousUsersRowMap = ImmutableMap.<String, String>builder()
                .put(Constants.COLUMN_HEALTH_CODE, "previous user's health code")
                .put(Constants.COLUMN_CREATED_DATE, DATE_STRING)
                .put(COLUMN_ID, "previous user's data")
                .build();

        PopulatedTable populatedTable = new PopulatedTable(TABLE_ID, TABLE_SCHEMA);
        populatedTable.getRowList().add(previousUsersRowMap);

        ctx.getPopulatedTablesById().put(TABLE_ID, populatedTable);

        // Make HTTP response.
        mockHttpResponse = "{\n" +
                "   \"" + TABLE_KEY + "\":{\n" +
                "       \"" + COLUMN_ID + "\":\"current user's data\"\n" +
                "   }\n" +
                "}";

        // Execute and validate
        processor.processEndpointForUser(ctx, user, ENDPOINT_SCHEMA);

        List<Map<String, String>> rowList = validatePopulatedTablesById();
        assertEquals(rowList.size(), 2);
        assertEquals(rowList.get(0), previousUsersRowMap);
        validateRow(rowList.get(1), "current user's data", 0);
        verifyFileHelperClean();

        verify(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        verify(processor, never()).warnWrapper(any());
    }

    @Test
    public void edgeCaseUnexpectedTableKey() throws Exception {
        // Make HTTP response.
        mockHttpResponse = "{\n" +
                "   \"wrong-table-key\":{\n" +
                "       \"any-column\":\"Any value\"\n" +
                "   }\n" +
                "}";

        // Execute and validate
        processor.processEndpointForUser(ctx, user, ENDPOINT_SCHEMA);
        assertTrue(ctx.getPopulatedTablesById().isEmpty());
        verify(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        verify(processor).warnWrapper("Unexpected table " + ENDPOINT_ID + ".wrong-table-key for user " +
                HEALTH_CODE);
    }

    @Test
    public void edgeCaseIgnoredKey() throws Exception {
        // Make HTTP response.
        mockHttpResponse = "{\n" +
                "   \"" + IGNORED_KEY + "\":{\n" +
                "       \"any-column\":\"Any value\"\n" +
                "   }\n" +
                "}";

        // Execute and validate
        processor.processEndpointForUser(ctx, user, ENDPOINT_SCHEMA);
        assertTrue(ctx.getPopulatedTablesById().isEmpty());
        verify(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        verify(processor, never()).warnWrapper(any());
    }

    @Test
    public void edgeCaseNeitherArrayNorObject() throws Exception {
        // Make HTTP response.
        mockHttpResponse = "{\n" +
                "   \"" + TABLE_KEY + "\":\"This is an invalid data type\"\n" +
                "}";

        // Execute and validate
        processor.processEndpointForUser(ctx, user, ENDPOINT_SCHEMA);

        List<Map<String, String>> rowList = validatePopulatedTablesById();
        assertTrue(rowList.isEmpty());

        verify(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        verify(processor).warnWrapper("Table " + TABLE_ID + " is neither array nor object for user " +
                HEALTH_CODE);
    }

    @Test
    public void edgeCaseUnexpectedColumnInTable() throws Exception {
        // Make HTTP response.
        mockHttpResponse = "{\n" +
                "   \"" + TABLE_KEY + "\":{\n" +
                "       \"wrong-column\":\"value is ignored\"\n" +
                "   }\n" +
                "}";

        // Execute and validate
        processor.processEndpointForUser(ctx, user, ENDPOINT_SCHEMA);

        // Row value map only has common fields and raw data.
        List<Map<String, String>> rowList = validatePopulatedTablesById();
        assertEquals(rowList.size(), 1);
        Map<String, String> rowValueMap = rowList.get(0);
        assertEquals(rowValueMap.size(), 3);
        assertEquals(rowValueMap.get(Constants.COLUMN_HEALTH_CODE), HEALTH_CODE);
        assertEquals(rowValueMap.get(Constants.COLUMN_CREATED_DATE), DATE_STRING);
        assertEquals(rowValueMap.get(Constants.COLUMN_RAW_DATA), FILEHANDLE_ID + 0);

        // Validate raw data.
        JsonNode rawDataNode = DefaultObjectMapper.INSTANCE.readTree(uploadedFileBytesList.get(0));
        assertEquals(rawDataNode.size(), 1);
        assertEquals(rawDataNode.get("wrong-column").textValue(), "value is ignored");
        verifyFileHelperClean();

        // Verify spied calls.
        verify(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        verify(processor).warnWrapper("Unexpected column wrong-column in table " + TABLE_ID + " for user " +
                HEALTH_CODE);
    }

    @Test
    public void edgeCaseEmptyRow() throws Exception {
        // Make HTTP response.
        mockHttpResponse = "{\n" +
                "   \"" + TABLE_KEY + "\":{}\n" +
                "}";

        // Execute and validate
        processor.processEndpointForUser(ctx, user, ENDPOINT_SCHEMA);

        List<Map<String, String>> rowList = validatePopulatedTablesById();
        assertTrue(rowList.isEmpty());

        verify(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        verify(processor, never()).warnWrapper(any());
    }

    @Test
    public void edgeCaseNullValue() throws Exception {
        // Make HTTP response.
        mockHttpResponse = "{\n" +
                "   \"" + TABLE_KEY + "\":{\n" +
                "       \"" + COLUMN_ID + "\":null\n" +
                "   }\n" +
                "}";

        // Execute and validate
        processor.processEndpointForUser(ctx, user, ENDPOINT_SCHEMA);

        // Row value map only has common fields and raw data.
        List<Map<String, String>> rowList = validatePopulatedTablesById();
        assertEquals(rowList.size(), 1);
        Map<String, String> rowValueMap = rowList.get(0);
        assertEquals(rowValueMap.size(), 3);
        assertEquals(rowValueMap.get(Constants.COLUMN_HEALTH_CODE), HEALTH_CODE);
        assertEquals(rowValueMap.get(Constants.COLUMN_CREATED_DATE), DATE_STRING);
        assertEquals(rowValueMap.get(Constants.COLUMN_RAW_DATA), FILEHANDLE_ID + 0);

        // Validate raw data.
        JsonNode rawDataNode = DefaultObjectMapper.INSTANCE.readTree(uploadedFileBytesList.get(0));
        assertEquals(rawDataNode.size(), 1);
        assertTrue(rawDataNode.get(COLUMN_ID).isNull());
        verifyFileHelperClean();

        // Verify spied calls.
        verify(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        verify(processor, never()).warnWrapper(any());
    }

    @Test
    public void http403Suppressed() throws Exception {
        doThrow(new HttpResponseException(403, "Forbidden")).when(processor).makeHttpRequest(URL, ACCESS_TOKEN);
        processor.processEndpointForUser(ctx, user, ENDPOINT_SCHEMA);
        assertTrue(ctx.getPopulatedTablesById().isEmpty());
    }

    @Test
    public void http500Suppressed() throws Exception {
        doThrow(new HttpResponseException(500, "Internal Server Error")).when(processor).makeHttpRequest(URL,
                ACCESS_TOKEN);
        processor.processEndpointForUser(ctx, user, ENDPOINT_SCHEMA);
        assertTrue(ctx.getPopulatedTablesById().isEmpty());
    }

    // Validate the PopulatedTablesById is correct, and returns the row list.
    private List<Map<String, String>> validatePopulatedTablesById() {
        Map<String, PopulatedTable> populatedTablesById = ctx.getPopulatedTablesById();
        assertEquals(populatedTablesById.size(), 1);

        PopulatedTable populatedTable = populatedTablesById.get(TABLE_ID);
        assertEquals(populatedTable.getTableId(), TABLE_ID);
        assertEquals(populatedTable.getTableSchema(), TABLE_SCHEMA);

        return populatedTable.getRowList();
    }

    private void validateRow(Map<String, String> rowValueMap, String expected, int expectedFileIndex)
            throws Exception {
        // Validate row map.
        assertEquals(rowValueMap.size(), 4);
        assertEquals(rowValueMap.get(Constants.COLUMN_HEALTH_CODE), HEALTH_CODE);
        assertEquals(rowValueMap.get(Constants.COLUMN_CREATED_DATE), DATE_STRING);
        assertEquals(rowValueMap.get(Constants.COLUMN_RAW_DATA), FILEHANDLE_ID + expectedFileIndex);
        assertEquals(rowValueMap.get(COLUMN_ID), expected);

        // Validate raw data. For this test, raw data is always an object with a single key COLUMN_ID with String value
        // expected.
        JsonNode rawDataNode = DefaultObjectMapper.INSTANCE.readTree(uploadedFileBytesList.get(expectedFileIndex));
        assertEquals(rawDataNode.size(), 1);
        assertEquals(rawDataNode.get(COLUMN_ID).textValue(), expected);
    }

    private void verifyFileHelperClean() {
        inMemoryFileHelper.deleteDir(ctx.getTmpDir());
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    @DataProvider(name = "serializeDataProvider")
    public Object[][] serializeDataProvider() {
        return new Object[][] {
                { null, STRING_COLUMN, null },
                { NullNode.instance, STRING_COLUMN, null },
                { new IntNode(1), BOOLEAN_COLUMN, null },
                { new TextNode("true"), BOOLEAN_COLUMN, null },
                { BooleanNode.TRUE, BOOLEAN_COLUMN, "true" },
                { new LongNode(1513152387123L), DATE_COLUMN, null },
                { new TextNode("December 12, 2017 at 18:56:51"), DATE_COLUMN, null },
                { new TextNode("2017-12-12T18:56:51.098Z"), DATE_COLUMN, "1513105011098" },
                { new TextNode("3.14159"), DOUBLE_COLUMN, null },
                { new DecimalNode(new BigDecimal("3.14159")), DOUBLE_COLUMN, "3.14159" },
                { new IntNode(3), DOUBLE_COLUMN, "3" },
                { new TextNode("42"), INTEGER_COLUMN, null },
                { new IntNode(42), INTEGER_COLUMN, "42" },
                { new DecimalNode(new BigDecimal("3.14159")), INTEGER_COLUMN, "3" },
                { new IntNode(1024), STRING_COLUMN, "1024" },
                { new TextNode("foo"), STRING_COLUMN, "foo" },
                { new TextNode("aaaabbbbccccdddd"), STRING_COLUMN, "aaaabbbbcc" },
                { new TextNode("foo"), UNSUPPORTED_COLUMN, null },
        };
    }

    @Test(dataProvider = "serializeDataProvider")
    public void serialize(JsonNode node, ColumnSchema columnSchema, String expected) throws Exception {
        String result = processor.serializeJsonForColumn(ctx, node, columnSchema);
        assertEquals(result, expected);
    }

    @Test
    public void serializeLargeText() throws Exception {
        ObjectNode node = DefaultObjectMapper.INSTANCE.createObjectNode();
        node.put("foo", "foo-value");
        node.put("bar", "bar-value");
        String result = processor.serializeJsonForColumn(ctx, node, LARGETEXT_COLUMN);
        assertEquals(result, node.toString());
    }

    @Test
    public void serializeFileHandler() throws Exception {
        // Set up JSON value
        ObjectNode node = DefaultObjectMapper.INSTANCE.createObjectNode();
        node.put("foo", "foo-value");
        node.put("bar", "bar-value");

        // Execute and validate
        String result = processor.serializeJsonForColumn(ctx, node, FILEHANDLE_COLUMN);
        assertEquals(result, FILEHANDLE_ID + 0);

        // Validate uploaded file contents
        JsonNode uploadedNode = DefaultObjectMapper.INSTANCE.readTree(uploadedFileBytesList.get(0));
        assertEquals(uploadedNode, node);

        // Make sure we deleted the file when we are done
        ArgumentCaptor<File> uploadedFileCaptor = ArgumentCaptor.forClass(File.class);
        verify(mockSynapseHelper).createFileHandleWithRetry(uploadedFileCaptor.capture());
        File uploadedFile = uploadedFileCaptor.getValue();
        assertFalse(inMemoryFileHelper.fileExists(uploadedFile));
    }
}
