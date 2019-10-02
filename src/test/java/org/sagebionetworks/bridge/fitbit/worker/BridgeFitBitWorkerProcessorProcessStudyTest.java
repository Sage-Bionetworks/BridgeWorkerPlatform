package org.sagebionetworks.bridge.fitbit.worker;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.fitbit.schema.EndpointSchema;
import org.sagebionetworks.bridge.fitbit.schema.TableSchema;
import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.rest.model.OAuthAccessToken;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.FitBitUser;
import org.sagebionetworks.bridge.workerPlatform.exceptions.WorkerException;

@SuppressWarnings({ "ResultOfMethodCallIgnored", "unchecked" })
public class BridgeFitBitWorkerProcessorProcessStudyTest {
    private static final String DATE_STRING = "2017-12-11";
    private static final List<String> SCOPE_LIST = ImmutableList.of("ENDPOINT_0", "ENDPOINT_1", "ENDPOINT_2");
    private static final String STUDY_ID = "test-study";
    private static final Study STUDY = new Study().identifier(STUDY_ID);

    private InMemoryFileHelper fileHelper;
    private BridgeHelper mockBridgeHelper;
    private TableProcessor mockTableProcessor;
    private UserProcessor mockUserProcessor;
    private BridgeFitBitWorkerProcessor processor;

    @BeforeMethod
    public void setup() {
        // Mock back-ends
        fileHelper = new InMemoryFileHelper();
        mockBridgeHelper = mock(BridgeHelper.class);
        mockTableProcessor = mock(TableProcessor.class);
        mockUserProcessor = mock(UserProcessor.class);

        // Set up FitBit Worker Processor.
        processor = new BridgeFitBitWorkerProcessor();
        processor.setBridgeHelper(mockBridgeHelper);
        processor.setFileHelper(fileHelper);
        processor.setTableProcessor(mockTableProcessor);
        processor.setUserProcessor(mockUserProcessor);

        // Set rate limit to 1000 so tests aren't bottlenecked by the rate limiter.
        processor.setPerUserRateLimit(1000.0);
    }

    @Test
    public void multipleUsersFromBridge() throws Exception {
        // Test cases: First user throws. Second and third users succeed.

        // Mock BridgeHelper to return users.
        FitBitUser user0 = makeUser(0);
        FitBitUser user1 = makeUser(1);
        FitBitUser user2 = makeUser(2);
        when(mockBridgeHelper.getFitBitUsersForStudy(STUDY_ID)).thenReturn(Iterators.forArray(user0, user1, user2));

        // Mock endpoint schema, so we don't have to construct the whole thing.

        EndpointSchema mockEndpointSchema0 = mockEndpointSchema(0);
        processor.setEndpointSchemas(ImmutableList.of(mockEndpointSchema0));

        // Mock user processor to set up one table in the context.
        doAnswer(invocation -> {
            // user-0 throws
            FitBitUser user = invocation.getArgumentAt(1, FitBitUser.class);
            if ("user-0".equals(user.getUserId())) {
                throw new RuntimeException("test exception");
            }

            // Ensure that there's a PopulatedTable for this endpoint.
            RequestContext ctx = invocation.getArgumentAt(0, RequestContext.class);
            EndpointSchema endpointSchema = invocation.getArgumentAt(2, EndpointSchema.class);
            String tableId = endpointSchema.getEndpointId() + "-table";
            ctx.getPopulatedTablesById().computeIfAbsent(tableId, key -> new PopulatedTable(tableId, mock(
                    TableSchema.class)));

            // Required return value.
            return null;
        }).when(mockUserProcessor).processEndpointForUser(any(), any(), any());

        // Execute
        processor.processStudy(DATE_STRING, STUDY, null);

        // Verify User Processor
        ArgumentCaptor<RequestContext> contextCaptor = ArgumentCaptor.forClass(RequestContext.class);
        ArgumentCaptor<FitBitUser> userCaptor = ArgumentCaptor.forClass(FitBitUser.class);
        verify(mockUserProcessor, times(3)).processEndpointForUser(contextCaptor.capture(),
                userCaptor.capture(), same(mockEndpointSchema0));

        // RequestContext contains date and studyID
        List<RequestContext> contextList = contextCaptor.getAllValues();
        assertEquals(contextList.size(), 3);
        RequestContext context0 = contextList.get(0);
        assertEquals(context0.getDate(), DATE_STRING);
        assertSame(context0.getStudy(), STUDY);

        // All contexts within the study are the same context.
        assertSame(contextList.get(1), context0);
        assertSame(contextList.get(2), context0);

        // Validate users
        List<FitBitUser> userList = userCaptor.getAllValues();
        assertEquals(userList.size(), 3);
        assertSame(userList.get(0), user0);
        assertSame(userList.get(1), user1);
        assertSame(userList.get(2), user2);

        // Verify Table Processor
        ArgumentCaptor<PopulatedTable> tableCaptor = ArgumentCaptor.forClass(PopulatedTable.class);
        verify(mockTableProcessor, times(1)).processTable(same(context0),
                tableCaptor.capture());
        assertEquals(tableCaptor.getValue().getTableId(), "endpoint-0-table");

        // Validate we cleaned up the file helper
        assertTrue(fileHelper.isEmpty());
    }

    @Test
    public void multipleUsersFromWhitelist() throws Exception {
        // Test cases: First user throws on getting the FitBitUser. Second and third users succeed.

        // Mock BridgeHelper to return users.
        when(mockBridgeHelper.getFitBitUserForStudyAndHealthCode(STUDY_ID, "health-code-0"))
                .thenThrow(IOException.class);

        when(mockBridgeHelper.getFitBitUserForStudyAndHealthCode(STUDY_ID, "health-code-1"))
                .thenThrow(RuntimeException.class);

        FitBitUser user2 = makeUser(2);
        when(mockBridgeHelper.getFitBitUserForStudyAndHealthCode(STUDY_ID, "health-code-2"))
                .thenReturn(user2);

        FitBitUser user3 = makeUser(3);
        when(mockBridgeHelper.getFitBitUserForStudyAndHealthCode(STUDY_ID, "health-code-3"))
                .thenReturn(user3);

        // Mock endpoint schema, so we don't have to construct the whole thing.
        EndpointSchema mockEndpointSchema0 = mockEndpointSchema(0);
        processor.setEndpointSchemas(ImmutableList.of(mockEndpointSchema0));

        // Mock user processor to do nothing. This is thoroughly tested in other tests.
        doNothing().when(mockUserProcessor).processEndpointForUser(any(), any(), any());

        // Execute.
        processor.processStudy(DATE_STRING, STUDY, ImmutableList.of("health-code-0", "health-code-1",
                "health-code-2", "health-code-3"));

        // Verify User Processor. Because user-0 and user-2 throws while trying to get a FitBitUser, we never call the
        // User Processor for those users.
        ArgumentCaptor<FitBitUser> userCaptor = ArgumentCaptor.forClass(FitBitUser.class);
        verify(mockUserProcessor, times(2)).processEndpointForUser(any(),
                userCaptor.capture(), same(mockEndpointSchema0));

        List<FitBitUser> userList = userCaptor.getAllValues();
        assertEquals(userList.size(), 2);
        assertSame(userList.get(0), user2);
        assertSame(userList.get(1), user3);

        // Validate we cleaned up the file helper
        assertTrue(fileHelper.isEmpty());
    }

    // branch coverage
    @Test
    public void emptyHealthCodeWhitelist() throws Exception {
        // Mock BridgeHelper to return users.
        FitBitUser user3 = makeUser(3);
        when(mockBridgeHelper.getFitBitUsersForStudy(STUDY_ID)).thenReturn(Iterators.forArray(user3));

        // Mock endpoint schema, so we don't have to construct the whole thing.
        EndpointSchema mockEndpointSchema0 = mockEndpointSchema(0);
        processor.setEndpointSchemas(ImmutableList.of(mockEndpointSchema0));

        // Mock user processor to do nothing. This is thoroughly tested in other tests.
        doNothing().when(mockUserProcessor).processEndpointForUser(any(), any(), any());

        // Execute.
        processor.processStudy(DATE_STRING, STUDY, ImmutableList.of());

        // Verify User Processor.
        ArgumentCaptor<FitBitUser> userCaptor = ArgumentCaptor.forClass(FitBitUser.class);
        verify(mockUserProcessor).processEndpointForUser(any(), userCaptor.capture(), same(mockEndpointSchema0));
        assertSame(userCaptor.getValue(), user3);

        // Validate we cleaned up the file helper
        assertTrue(fileHelper.isEmpty());
    }

    @Test
    public void multipleEndpointsAndTables() throws Exception {
        // Test cases:
        //   First endpoint throws. Second endpoint has 2 tables. Third has 1 table.
        //   First table throws. Second and third table succeed.

        // Mock BridgeHelper to return users.
        FitBitUser user0 = makeUser(0);
        when(mockBridgeHelper.getFitBitUsersForStudy(STUDY_ID)).thenReturn(Iterators.forArray(user0));

        // Mock endpoint schemas, so we don't have to construct the whole thing. Note that we have 4 endpoints, but the
        // user is configured with only endpoints 0-2.
        EndpointSchema mockEndpointSchema0 = mockEndpointSchema(0);
        EndpointSchema mockEndpointSchema1 = mockEndpointSchema(1);
        EndpointSchema mockEndpointSchema2 = mockEndpointSchema(2);
        EndpointSchema mockEndpointSchema3 = mockEndpointSchema(3);
        processor.setEndpointSchemas(ImmutableList.of(mockEndpointSchema0, mockEndpointSchema1, mockEndpointSchema2,
                mockEndpointSchema3));

        // Mock user processor to set up one table in the context.
        doAnswer(invocation -> {
            // Process endpoints for test.
            EndpointSchema endpointSchema = invocation.getArgumentAt(2, EndpointSchema.class);
            List<String> tableIdList = new ArrayList<>();
            switch (endpointSchema.getEndpointId()) {
                case "endpoint-0":
                    // Throws
                    throw new RuntimeException("test exception");
                case "endpoint-1":
                    // Has 2 tables
                    tableIdList.add("table-1A");
                    tableIdList.add("table-1B");
                    break;
                case "endpoint-2":
                    // Has 1 table
                    tableIdList.add("table-2");
                    break;
            }

            // Ensure PopulatedTables for endpoint.
            RequestContext ctx = invocation.getArgumentAt(0, RequestContext.class);
            for (String oneTableId : tableIdList) {
                ctx.getPopulatedTablesById().computeIfAbsent(oneTableId, key -> new PopulatedTable(oneTableId, mock(
                        TableSchema.class)));
            }

            // Required return value
            return null;
        }).when(mockUserProcessor).processEndpointForUser(any(), any(), any());

        // Mock table processor. table-1A throws.
        doAnswer(invocation -> {
            PopulatedTable table = invocation.getArgumentAt(1, PopulatedTable.class);
            if ("table-1A".equals(table.getTableId())) {
                throw new RuntimeException("test exception");
            }

            // Required return value
            return null;
        }).when(mockTableProcessor).processTable(any(), any());

        // Execute
        processor.processStudy(DATE_STRING, STUDY, null);

        // Verify User Processor
        ArgumentCaptor<RequestContext> contextCaptor = ArgumentCaptor.forClass(RequestContext.class);
        ArgumentCaptor<EndpointSchema> endpointSchemaCaptor = ArgumentCaptor.forClass(EndpointSchema.class);
        verify(mockUserProcessor, times(3)).processEndpointForUser(contextCaptor.capture(),
                same(user0), endpointSchemaCaptor.capture());

        // RequestContext contains date and studyID
        List<RequestContext> contextList = contextCaptor.getAllValues();
        assertEquals(contextList.size(), 3);
        RequestContext context0 = contextList.get(0);
        assertEquals(context0.getDate(), DATE_STRING);
        assertSame(context0.getStudy(), STUDY);

        // All contexts within the study are the same context.
        assertSame(contextList.get(1), context0);
        assertSame(contextList.get(2), context0);

        // Validate endpoint schemas
        List<EndpointSchema> endpointSchemaList = endpointSchemaCaptor.getAllValues();
        assertEquals(endpointSchemaList.size(), 3);
        assertEquals(endpointSchemaList.get(0), mockEndpointSchema0);
        assertEquals(endpointSchemaList.get(1), mockEndpointSchema1);
        assertEquals(endpointSchemaList.get(2), mockEndpointSchema2);

        // Verify Table Processor
        ArgumentCaptor<PopulatedTable> tableCaptor = ArgumentCaptor.forClass(PopulatedTable.class);
        verify(mockTableProcessor, times(3)).processTable(same(context0),
                tableCaptor.capture());

        Set<String> tableIdSet = tableCaptor.getAllValues().stream().map(PopulatedTable::getTableId).collect(Collectors
                .toSet());
        assertEquals(tableIdSet.size(), 3);
        assertTrue(tableIdSet.contains("table-1A"));
        assertTrue(tableIdSet.contains("table-1B"));
        assertTrue(tableIdSet.contains("table-2"));

        // Validate we cleaned up the file helper
        assertTrue(fileHelper.isEmpty());
    }

    @Test
    public void errorsGettingNextUsers() {
        // Test cases: FitBitUserIterator keeps throwing on next(). We abort when we hit the error limit.

        // Set user error limit to something small, to make it easier to test.
        processor.setUserErrorLimit(3);

        // Mock BridgeHelper to return an iterator that always throws.
        Iterator<FitBitUser> mockIterator = mock(Iterator.class);
        when(mockIterator.hasNext()).thenReturn(true);
        when(mockIterator.next()).thenThrow(new BridgeSDKException("mock Bridge down", 503));
        when(mockBridgeHelper.getFitBitUsersForStudy(STUDY_ID)).thenReturn(mockIterator);

        // Mock endpoint schema. We're never going to use this, but if for some reason the test fails, we want there
        // to be an inner loop that we can verify is never called.
        EndpointSchema mockEndpointSchema0 = mockEndpointSchema(0);
        processor.setEndpointSchemas(ImmutableList.of(mockEndpointSchema0));

        // Execute (throws exception).
        try {
            processor.processStudy(DATE_STRING, STUDY, null);
            fail("expected exception");
        } catch (WorkerException ex) {
            assertEquals(ex.getMessage(), "User error limit reached, aborting for study " + STUDY_ID);
        }

        // We call the iterator exactly 3 times.
        verify(mockIterator, times(3)).hasNext();
        verify(mockIterator, times(3)).next();

        // User processor is never called.
        verifyZeroInteractions(mockUserProcessor);

        // Validate we cleaned up the file helper
        assertTrue(fileHelper.isEmpty());
    }

    private static FitBitUser makeUser(int idx) {
        // Mock OAuth token. This is read-only, so it's easier to just mock it instead of using Reflection.
        OAuthAccessToken mockOauthToken = mock(OAuthAccessToken.class);
        when(mockOauthToken.getAccessToken()).thenReturn("access-token-" + idx);
        when(mockOauthToken.getProviderUserId()).thenReturn("user-" + idx);
        when(mockOauthToken.getScopes()).thenReturn(SCOPE_LIST);

        return new FitBitUser.Builder().withHealthCode("health-code-" + idx).withToken(mockOauthToken).build();
    }

    private static EndpointSchema mockEndpointSchema(int idx) {
        EndpointSchema mockEndpointSchema = mock(EndpointSchema.class);
        when(mockEndpointSchema.getEndpointId()).thenReturn("endpoint-" + idx);
        when(mockEndpointSchema.getScopeName()).thenReturn("ENDPOINT_" + idx);
        return mockEndpointSchema;
    }
}
