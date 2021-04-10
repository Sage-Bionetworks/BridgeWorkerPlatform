package org.sagebionetworks.bridge.participantroster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.Phone;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.udd.helper.SesHelper;
import org.sagebionetworks.bridge.udd.helper.ZipHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.AccountInfo;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.AppInfo;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class DownloadParticipantRosterWorkerProcessorTest {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final String APP_ID = "test-app";
    private static final String USER_ID = "test-user-id";
    private static final String PASSWORD = "test-password";

    private DynamoHelper mockDynamoHelper;
    private BridgeHelper mockBridgeHelper;
    private SesHelper mockSesHelper;
    private InMemoryFileHelper fileHelper;
    private ZipHelper mockZipHelper;

    private static DateTime now = DateTime.now();

    private DownloadParticipantRosterWorkerProcessor processor;

    @BeforeMethod
    public void before() {
        mockDynamoHelper = mock(DynamoHelper.class);
        mockBridgeHelper = mock(BridgeHelper.class);
        mockSesHelper = mock(SesHelper.class);
        fileHelper = new InMemoryFileHelper();
        mockZipHelper = mock(ZipHelper.class);

        processor = spy(new DownloadParticipantRosterWorkerProcessor());
        processor.setDynamoHelper(mockDynamoHelper);
        processor.setSesHelper(mockSesHelper);
        processor.setFileHelper(fileHelper);
        processor.setBridgeHelper(mockBridgeHelper);
        processor.setZipHelper(mockZipHelper);
    }

    // test not a researcher OR study coordinator
    @Test
    public void notResearcherOrCoordinator() throws Exception {
        // initialize participant with no Researcher or Study Coordinator role
        StudyParticipant participant = new StudyParticipant();

        doReturn(participant).when(mockBridgeHelper).getParticipant(APP_ID, USER_ID, false);

        processor.accept(makeValidRequestNode());

        // verify that this call isn't reached, because the participant isn't a researcher or study coordinator
        verify(mockBridgeHelper, never()).getAccountSummariesForApp(anyString(), anyString(), anyInt(), anyInt(), anyString());
    }

    @Test
    public void noEmail() throws Exception {
        StudyParticipant participant = new StudyParticipant();
        participant.addRolesItem(Role.RESEARCHER);

        doReturn(participant).when(mockBridgeHelper).getParticipant(APP_ID, USER_ID, false);
        doReturn(new ArrayList<AccountSummary>()).when(mockBridgeHelper).getAccountSummariesForApp(
                anyString(), anyString(), anyInt(), anyInt(), anyString());

        processor.accept(makeValidRequestNode());

        // verify that this call isn't reached, because study participant does not have an email
        verify(mockBridgeHelper, never()).getAccountSummariesForApp(anyString(), anyString(), anyInt(), anyInt(), anyString());
    }

    @Test
    public void emailNotVerified() throws Exception {
        StudyParticipant participant = new StudyParticipant();
        participant.setEmail("example@example.org");
        participant.setEmailVerified(false);
        participant.addRolesItem(Role.RESEARCHER);

        doReturn(participant).when(mockBridgeHelper).getParticipant(APP_ID, USER_ID, false);
        doReturn(new ArrayList<AccountSummary>()).when(mockBridgeHelper).getAccountSummariesForApp(
                anyString(), anyString(), anyInt(), anyInt(), anyString());

        processor.accept(makeValidRequestNode());

        // verify that this call isn't reached, because study participant does not have a verified email
        verify(mockBridgeHelper, never()).getAccountSummariesForApp(anyString(), anyString(), anyInt(), anyInt(), anyString());
    }

    @Test
    public void getCsvHeaders() throws IllegalAccessException {
        String[] headers = processor.getCsvHeaders();
        assertAccountSummaryHeaders(headers);
    }

    @Test
    public void getAccountSummaryArray() throws IllegalAccessException {
        AccountSummary accountSummary = makeAccountSummary();

        String[] headers = processor.getCsvHeaders();
        assertAccountSummaryHeaders(headers);

        String[] accountSummaryArray = processor.getAccountSummaryArray(accountSummary, headers);
        assertEquals(accountSummaryArray.length, 14);

        assertEquals("test-first-name", accountSummaryArray[0]);
        assertEquals("test-last-name", accountSummaryArray[1]);
        assertEquals("test@test.test", accountSummaryArray[2]);
        assertEquals("", accountSummaryArray[3]);
        assertEquals("", accountSummaryArray[4]);
        assertEquals(now.toString(), accountSummaryArray[5]);
        assertEquals("", accountSummaryArray[6]);
        assertEquals("test-app", accountSummaryArray[7]);
        assertEquals("", accountSummaryArray[8]);
        assertEquals("", accountSummaryArray[9]);
        assertEquals("", accountSummaryArray[10]);
        assertEquals("", accountSummaryArray[11]);
        assertEquals("test-orgId", accountSummaryArray[12]);
        assertEquals("", accountSummaryArray[13]);
    }

    @Test
    public void normalCase() throws Exception {
        // default pageSize is 100
        verifyNormalCase();
    }

    @Test
    public void normalCaseMultiplePages() throws Exception {
        // set page size
        processor.setPageSize(5);

        verifyNormalCase();
    }

    private void verifyNormalCase() throws Exception {
        // set up participant
        StudyParticipant participant = new StudyParticipant();
        participant.setEmail("example@example.org");
        participant.setEmailVerified(true);
        participant.addRolesItem(Role.RESEARCHER);

        //set up mocks
        doReturn(participant).when(mockBridgeHelper).getParticipant(APP_ID, USER_ID, false);

        when(mockBridgeHelper.getAccountSummariesForApp(anyString(), anyString(), anyInt(), anyInt(), anyString()))
                .thenReturn(ImmutableList.of(makeAccountSummary(), makeAccountSummary(), makeAccountSummary()))
                .thenReturn(new ArrayList<>());

        doNothing().when(mockZipHelper).zipWithPassword(anyList(), anyString(), anyString());

        // execute the processor
        processor.accept(makeValidRequestNode());

        // verify that the csv is written to the file
        verify(processor).writeAccountSummaries(any(), anyList(), anyInt(), anyString(), anyString(), anyString());

        // verify that the file is zipped
        verify(mockZipHelper).zipWithPassword(anyList(), anyString(), anyString());

        // verify that the email with attachment is sent
        verify(mockSesHelper).sendEmailWithAttachmentToAccount(any(AppInfo.class), any(AccountInfo.class), anyString());
    }

    public void assertAccountSummaryHeaders(String[] headers) {
        assertEquals(headers.length, 14);
        assertEquals("firstName", headers[0]);
        assertEquals("lastName", headers[1]);
        assertEquals("email", headers[2]);
        assertEquals("phone", headers[3]);
        assertEquals("id", headers[4]);
        assertEquals("createdOn", headers[5]);
        assertEquals("status", headers[6]);
        assertEquals("appId", headers[7]);
        assertEquals("studyIds", headers[8]);
        assertEquals("externalIds", headers[9]);
        assertEquals("synapseUserId", headers[10]);
        assertEquals("attributes", headers[11]);
        assertEquals("orgMembership", headers[12]);
        assertEquals("type", headers[13]);
    }

    private static ObjectNode makeValidRequestNode() {
        ObjectNode requestNode = JSON_MAPPER.createObjectNode();
        requestNode.put("appId", APP_ID);
        requestNode.put("userId", USER_ID);
        requestNode.put("password", PASSWORD);

        return requestNode;
    }

    private static AccountSummary makeAccountSummary() throws IllegalAccessException {
        AccountSummary accountSummary = new AccountSummary();
        setVariableValueInObject(accountSummary, "firstName", "test-first-name");
        setVariableValueInObject(accountSummary, "lastName", "test-last-name");
        setVariableValueInObject(accountSummary, "email", "test@test.test");
        setVariableValueInObject(accountSummary, "createdOn", now);
        setVariableValueInObject(accountSummary, "status", null);
        setVariableValueInObject(accountSummary, "appId", "test-app");
        setVariableValueInObject(accountSummary, "orgMembership", "test-orgId");

        return accountSummary;
    }

    private static void setVariableValueInObject(Object object, String variable, Object value) throws IllegalAccessException {
        Field field = getFieldByNameIncludingSuperclasses(variable, object.getClass());
        field.setAccessible(true);
        field.set(object, value);
    }

    @SuppressWarnings("rawtypes")
    private static Field getFieldByNameIncludingSuperclasses(String fieldName, Class clazz) {
        Field retValue = null;
        try {
            retValue = clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            Class superclass = clazz.getSuperclass();
            if (superclass != null) {
                retValue = getFieldByNameIncludingSuperclasses( fieldName, superclass );
            }
        }
        return retValue;
    }
}