package org.sagebionetworks.bridge.participantroster;

import au.com.bytecode.opencsv.CSVWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Stream;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class DownloadParticipantRosterWorkerProcessorTest {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final String APP_ID = "test-app";
    private static final String USER_ID = "test-user-id";
    private static final String PASSWORD = "test-password";
    private static final String ORG_ID = "test-org-id";
    private static final String STUDY_ID = "test-study-id";
    private static final int PAGE_SIZE = 100;

    private static final String EXPECTED_CSV_CONTENT =
            // header
            "\"firstName\",\"lastName\",\"id\",\"notifyByEmail\",\"attributes\",\"sharingScope\",\"createdOn\",\"emailVerified\",\"phoneVerified\",\"status\",\"roles\",\"dataGroups\",\"clientData\",\"languages\",\"studyIds\",\"externalIds\",\"healthCode\",\"email\",\"phone\",\"consentHistories\",\"consented\",\"timeZone\"\n" +
            // study participant 1
            "\"test-first-name1\",\"test-last-name\",\"test-id-12345\",\"true\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"{\"\"externalId1\"\":\"\"test-id-1\"\",\"\"externalId2\"\":\"\"test-id-2\"\",\"\"externalId3\"\":\"\"test-id-3\"\"}\",\"\",\"test@test.test\",\"\",\"\",\"\",\"\"\n" +
            // study participant 2
            "\"test-first-name2\",\"test-last-name\",\"test-id-12345\",\"true\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"{\"\"externalId1\"\":\"\"test-id-1\"\",\"\"externalId2\"\":\"\"test-id-2\"\",\"\"externalId3\"\":\"\"test-id-3\"\"}\",\"\",\"test@test.test\",\"\",\"\",\"\",\"\"\n" +
            // study participant 3
            "\"test-first-name3\",\"test-last-name\",\"test-id-12345\",\"true\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"{\"\"externalId1\"\":\"\"test-id-1\"\",\"\"externalId2\"\":\"\"test-id-2\"\",\"\"externalId3\"\":\"\"test-id-3\"\"}\",\"\",\"test@test.test\",\"\",\"\",\"\",\"\"\n";

    private DynamoHelper mockDynamoHelper;
    private BridgeHelper mockBridgeHelper;
    private SesHelper mockSesHelper;
    private InMemoryFileHelper fileHelper;
    private ZipHelper mockZipHelper;

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

    @Test
    public void notResearcherOrCoordinator() throws Exception {
        // initialize participant with no Researcher or Study Coordinator role
        StudyParticipant participant = new StudyParticipant();

        testParticipantRequirements(participant, makeValidRequestNode());
    }

    @Test
    public void noEmail() throws Exception {
        StudyParticipant participant = new StudyParticipant();
        participant.addRolesItem(Role.RESEARCHER);

        testParticipantRequirements(participant, makeValidRequestNode());

    }

    @Test
    public void emailNotVerified() throws Exception {
        StudyParticipant participant = new StudyParticipant();
        participant.setEmail("example@example.org");
        participant.setEmailVerified(false);
        participant.addRolesItem(Role.RESEARCHER);

        testParticipantRequirements(participant, makeValidRequestNode());

    }

    @Test
    public void studyCoordinatorWithoutOrgId() throws Exception {
        StudyParticipant participant = new StudyParticipant();
        participant.setEmail("example@example.org");
        participant.setEmailVerified(true);
        participant.addRolesItem(Role.STUDY_COORDINATOR);

        testParticipantRequirements(participant, makeValidRequestNode());
    }

    @Test
    public void studyIdNotSponsoredByOrg() throws Exception {
        StudyParticipant participant = new StudyParticipant();
        participant.setEmail("example@example.org");
        participant.setEmailVerified(true);
        participant.addRolesItem(Role.RESEARCHER);
        participant.setOrgMembership(ORG_ID);

        ObjectNode requestNode = makeValidRequestNode();
        requestNode.put("studyId", STUDY_ID);

        testParticipantRequirements(participant, requestNode);
    }

    @Test
    public void getCsvHeaders() throws IllegalAccessException {
        String[] headers = processor.getCsvHeaders();
        assertStudyParticipantHeaders(headers);
    }

    @Test
    public void getStudyParticipantArray() throws IllegalAccessException {
        StudyParticipant studyParticipant = makeStudyParticipant(1);

        String[] headers = processor.getCsvHeaders();
        assertStudyParticipantHeaders(headers);

        String[] studyParticipantArrayArray = processor.getStudyParticipantArray(studyParticipant, headers);
        assertEquals(studyParticipantArrayArray.length, 22);

        assertEquals("test-first-name1", studyParticipantArrayArray[0]);
        assertEquals("test-last-name", studyParticipantArrayArray[1]);
        assertEquals("test-id-12345", studyParticipantArrayArray[2]);
        assertEquals("true", studyParticipantArrayArray[3]);
        assertEquals("", studyParticipantArrayArray[4]);
        assertEquals("", studyParticipantArrayArray[6]);
        assertEquals("", studyParticipantArrayArray[7]);
        assertEquals("", studyParticipantArrayArray[8]);
        assertEquals("", studyParticipantArrayArray[9]);
        assertEquals("", studyParticipantArrayArray[10]);
        assertEquals("", studyParticipantArrayArray[11]);
        assertEquals("", studyParticipantArrayArray[12]);
        assertEquals("", studyParticipantArrayArray[13]);
        assertEquals("", studyParticipantArrayArray[14]);
        assertEquals("{\"externalId1\":\"test-id-1\",\"externalId2\":\"test-id-2\",\"externalId3\":\"test-id-3\"}", studyParticipantArrayArray[15]);
        assertEquals("", studyParticipantArrayArray[16]);
        assertEquals("test@test.test", studyParticipantArrayArray[17]);
        assertEquals("", studyParticipantArrayArray[18]);
        assertEquals("", studyParticipantArrayArray[19]);
        assertEquals("", studyParticipantArrayArray[20]);
        assertEquals("", studyParticipantArrayArray[21]);
    }

    @Test
    public void process() throws Exception {
        // set up participant
        StudyParticipant participant = new StudyParticipant();
        participant.setEmail("example@example.org");
        participant.setEmailVerified(true);
        participant.addRolesItem(Role.RESEARCHER);
        participant.setOrgMembership(ORG_ID);

        // set up files
        File tmpDir = fileHelper.createTempDir();
        File csvFile = fileHelper.newFile(tmpDir, "participant_roster.csv");
        File zipFile = fileHelper.newFile(tmpDir, "user_data.zip");

        // set up request
        DownloadParticipantRosterRequest request = new DownloadParticipantRosterRequest.Builder()
                .withUserId(USER_ID).withAppId(APP_ID).withPassword(PASSWORD).build();

        // set up mocks
        doReturn(participant).when(mockBridgeHelper).getParticipant(APP_ID, USER_ID, false);

        List<StudyParticipant> studyParticipants = ImmutableList.of(makeStudyParticipant(1), makeStudyParticipant(2), makeStudyParticipant(3));

        doReturn(studyParticipants).doReturn(ImmutableList.of()).when(mockBridgeHelper).getStudyParticipantsForApp(eq(APP_ID), eq(ORG_ID), eq(0), eq(PAGE_SIZE), anyString());

        // we want to evaluate the files before they're cleaned up
        doNothing().when(processor).cleanupFiles(csvFile, zipFile, tmpDir);

        // process
        processor.process(request, csvFile, zipFile);

        // verify file contents
        String csvContent = new String(fileHelper.getBytes(csvFile), StandardCharsets.UTF_8);
        assertEquals(EXPECTED_CSV_CONTENT, csvContent);

        // verify that the csv is written to the file
        verify(processor).writeStudyParticipants(any(CSVWriter.class), eq(studyParticipants), eq(0), eq(APP_ID), eq(ORG_ID), anyString());

        // verify that the file is zipped
        verify(mockZipHelper).zipWithPassword(anyList(), any(File.class), anyString());

        // verify that the email with attachment is sent
        verify(mockSesHelper).sendEmailWithAttachmentToAccount(any(AppInfo.class), any(AccountInfo.class), anyString());

        // cleanup files
        processor.cleanupFiles(csvFile, zipFile, tmpDir);
    }

    @Test
    public void paginatedProcess() throws Exception {
        // set up participant
        StudyParticipant participant = new StudyParticipant();
        participant.setEmail("example@example.org");
        participant.setEmailVerified(true);
        participant.addRolesItem(Role.RESEARCHER);
        participant.setOrgMembership(ORG_ID);

        // set up files
        File tmpDir = fileHelper.createTempDir();
        File csvFile = fileHelper.newFile(tmpDir, "participant_roster.csv");
        File zipFile = fileHelper.newFile(tmpDir, "user_data.zip");

        // set up request
        DownloadParticipantRosterRequest request = new DownloadParticipantRosterRequest.Builder()
                .withUserId(USER_ID).withAppId(APP_ID).withPassword(PASSWORD).build();

        // mock caller
        doReturn(participant).when(mockBridgeHelper).getParticipant(APP_ID, USER_ID, false);

        // mock multiple bridge calls to test pagination
        StudyParticipant participant1 = makeStudyParticipant(1);
        StudyParticipant participant2 = makeStudyParticipant(2);
        StudyParticipant participant3 = makeStudyParticipant(3);
        doAnswer(new Answer() {
            int count = 0;

            public Object answer(InvocationOnMock invocation) {
                if (count == 0) {
                    count++;
                    return ImmutableList.of(participant1);
                } else if (count == 1) {
                    count++;
                    return ImmutableList.of(participant2);
                } else if (count == 2) {
                    count++;
                    return ImmutableList.of(participant3);
                }
                return ImmutableList.of();
            }
        }).when(mockBridgeHelper).getStudyParticipantsForApp(eq(APP_ID), eq(ORG_ID), anyInt(), eq(1), anyString());


        // we want to evaluate the files before they're cleaned up
        doNothing().when(processor).cleanupFiles(csvFile, zipFile, tmpDir);

        // process
        processor.setPageSize(1); // the processor will need to make 3 Bridge API calls
        processor.process(request, csvFile, zipFile);

        // verify file contents
        String csvContent = new String(fileHelper.getBytes(csvFile), StandardCharsets.UTF_8);
        assertEquals(EXPECTED_CSV_CONTENT, csvContent);

        // verify that the csv is written to the file
        verify(processor).writeStudyParticipants(any(CSVWriter.class), anyList(), eq(0), eq(APP_ID), eq(ORG_ID), anyString());

        // verify that the file is zipped
        verify(mockZipHelper).zipWithPassword(anyList(), any(File.class), anyString());

        // verify that the email with attachment is sent
        verify(mockSesHelper).sendEmailWithAttachmentToAccount(any(AppInfo.class), any(AccountInfo.class), anyString());

        // cleanup files
        processor.cleanupFiles(csvFile, zipFile, tmpDir);
    }

    public void assertStudyParticipantHeaders(String[] headers) { // TODO update once we have final list of headers to omit
        assertEquals(headers.length, 22);
        assertEquals("firstName", headers[0]);
        assertEquals("lastName", headers[1]);
        assertEquals("id", headers[2]);
        assertEquals("notifyByEmail", headers[3]);
        assertEquals("attributes", headers[4]);
        assertEquals("sharingScope", headers[5]);
        assertEquals("createdOn", headers[6]);
        assertEquals("emailVerified", headers[7]);
        assertEquals("phoneVerified", headers[8]);
        assertEquals("status", headers[9]);
        assertEquals("roles", headers[10]);
        assertEquals("dataGroups", headers[11]);
        assertEquals("clientData", headers[12]);
        assertEquals("languages", headers[13]);
        assertEquals("studyIds", headers[14]);
        assertEquals("externalIds", headers[15]);
        assertEquals("healthCode", headers[16]);
        assertEquals("email", headers[17]);
        assertEquals("phone", headers[18]);
        assertEquals("consentHistories", headers[19]);
        assertEquals("consented", headers[20]);
        assertEquals("timeZone", headers[21]);
    }

    private static ObjectNode makeValidRequestNode() {
        ObjectNode requestNode = JSON_MAPPER.createObjectNode();
        requestNode.put("appId", APP_ID);
        requestNode.put("userId", USER_ID);
        requestNode.put("password", PASSWORD);

        return requestNode;
    }

    private static StudyParticipant makeStudyParticipant(int num) throws IllegalAccessException {
        StudyParticipant studyParticipant = new StudyParticipant();
        setVariableValueInObject(studyParticipant, "firstName", "test-first-name" + num);
        setVariableValueInObject(studyParticipant, "lastName", "test-last-name");
        setVariableValueInObject(studyParticipant, "email", "test@test.test");
        setVariableValueInObject(studyParticipant, "id", "test-id-12345");
        Map<String, String> externalIds = ImmutableMap.of("externalId1", "test-id-1",
                "externalId2", "test-id-2","externalId3", "test-id-3");
        setVariableValueInObject(studyParticipant, "externalIds", externalIds);

        return studyParticipant;
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

    private void testParticipantRequirements(StudyParticipant participant, ObjectNode requestNode) throws Exception {
        doReturn(participant).when(mockBridgeHelper).getParticipant(APP_ID, USER_ID, false);
        doReturn(new ArrayList<StudyParticipant>()).when(mockBridgeHelper).getStudyParticipantsForApp(
                anyString(), anyString(), anyInt(), anyInt(), anyString());

        processor.accept(requestNode);

        // verify that this call isn't reached, because study participant does not have a verified email
        verify(mockBridgeHelper, never()).getStudyParticipantsForApp(anyString(), anyString(), anyInt(), anyInt(), anyString());
    }
}