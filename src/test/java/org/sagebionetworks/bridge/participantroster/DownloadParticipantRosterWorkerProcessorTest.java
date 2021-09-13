package org.sagebionetworks.bridge.participantroster;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.Study;
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
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

public class DownloadParticipantRosterWorkerProcessorTest {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final String APP_ID = "test-app";
    private static final String USER_ID = "test-user-id";
    private static final String PASSWORD = "test-password";
    private static final String ORG_ID = "test-org-id";
    private static final String STUDY_ID = "test-study-id";
    private static final int PAGE_SIZE = 100;

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
    public void getStudyParticipantArray() throws IllegalAccessException {
        StudyParticipant studyParticipant = makeStudyParticipant(1);

        String[] headers = processor.getCsvHeaders();

        String[] studyParticipantArrayArray = processor.getStudyParticipantArray(studyParticipant, headers);
        Map<String, String> participantAttributeMap = parseCsvRowIntoMap(headers, studyParticipantArrayArray);

        assertEquals(participantAttributeMap.get("firstName"), "test-first-name1");
        assertEquals(participantAttributeMap.get("lastName"), "test-last-name");
        assertEquals(participantAttributeMap.get("id"), "test-id-12345");
        assertEquals(participantAttributeMap.get("notifyByEmail"), "true");
        assertEquals(participantAttributeMap.get("externalIds"),
                "{\"externalId1\":\"test-id-1\",\"externalId2\":\"test-id-2\",\"externalId3\":\"test-id-3\"}");
        assertEquals(participantAttributeMap.get("email"), "test@test.test");
    }

    @Test
    public void processWithoutStudyId() throws Exception {
        process(null);
    }

    @Test
    public void processWithStudyId() throws Exception {
        process(STUDY_ID);
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

        doReturn(ImmutableList.of(participant1)).when(mockBridgeHelper).getStudyParticipantsForApp(APP_ID, ORG_ID, 0, 1, null);
        doReturn(ImmutableList.of(participant2)).when(mockBridgeHelper).getStudyParticipantsForApp(APP_ID, ORG_ID, 1, 1, null);
        doReturn(ImmutableList.of(participant3)).when(mockBridgeHelper).getStudyParticipantsForApp(APP_ID, ORG_ID, 2, 1, null);

        AppInfo appInfo = new AppInfo.Builder().withAppId(APP_ID).withName("test-name").withSupportEmail("example@example.org").build();
        doReturn(appInfo).when(mockDynamoHelper).getApp(APP_ID);

        AccountInfo accountInfo = new AccountInfo.Builder().withUserId(USER_ID).withEmailAddress("example@example.org").build();
        doReturn(accountInfo).when(mockBridgeHelper).getAccountInfo(APP_ID, USER_ID);

        // we want to evaluate the files before they're cleaned up
        doNothing().when(processor).cleanupFiles(csvFile, zipFile, tmpDir);

        // process
        processor.setPageSize(1); // the processor will need to make 3 Bridge API calls
        processor.process(request, csvFile, zipFile, tmpDir);

        // verify file contents
        String csvContent = new String(fileHelper.getBytes(csvFile), StandardCharsets.UTF_8);
        assertCsvContent(csvContent);

        // verify that the csv is written to the file
        verify(processor).writeStudyParticipants(any(CSVWriter.class), eq(ImmutableList.of(participant1)), eq(0),
                eq(APP_ID), eq(ORG_ID), eq(null));

        // verify that the file is zipped
        verify(mockZipHelper).zipWithPassword(ImmutableList.of(csvFile), zipFile, PASSWORD);

        // verify that the email with attachment is sent
        verify(mockSesHelper).sendEmailWithAttachmentToAccount(appInfo, accountInfo, zipFile.getAbsolutePath());

        // cleanup files
        processor.cleanupFiles(csvFile, zipFile, tmpDir);
    }

    private void process(String studyId) throws Exception {
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
                .withUserId(USER_ID).withAppId(APP_ID).withPassword(PASSWORD).withStudyId(studyId).build();

        // set up mocks
        doReturn(participant).when(mockBridgeHelper).getParticipant(APP_ID, USER_ID, false);

        Study study = new Study();
        study.setIdentifier(studyId);
        List<Study> studies = ImmutableList.of(study);
        doReturn(studies).when(mockBridgeHelper).getSponsoredStudiesForApp(APP_ID, ORG_ID, 0, PAGE_SIZE);

        List<StudyParticipant> studyParticipants = ImmutableList.of(makeStudyParticipant(1), makeStudyParticipant(2), makeStudyParticipant(3));
        doReturn(studyParticipants).doReturn(ImmutableList.of()).when(mockBridgeHelper).getStudyParticipantsForApp(APP_ID, ORG_ID, 0, PAGE_SIZE, studyId);

        AppInfo appInfo = new AppInfo.Builder().withAppId(APP_ID).withName("test-name").withSupportEmail("example@example.org").build();
        doReturn(appInfo).when(mockDynamoHelper).getApp(APP_ID);

        AccountInfo accountInfo = new AccountInfo.Builder().withUserId(USER_ID).withEmailAddress("example@example.org").build();
        doReturn(accountInfo).when(mockBridgeHelper).getAccountInfo(APP_ID, USER_ID);

        // we want to evaluate the files before they're cleaned up
        doNothing().when(processor).cleanupFiles(csvFile, zipFile, tmpDir);

        // process
        processor.process(request, csvFile, zipFile, tmpDir);

        // verify file contents
        String csvContent = new String(fileHelper.getBytes(csvFile), StandardCharsets.UTF_8);
        assertCsvContent(csvContent);

        // verify that the csv is written to the file
        verify(processor).writeStudyParticipants(any(CSVWriter.class), eq(studyParticipants), eq(0), eq(APP_ID), eq(ORG_ID), eq(studyId));

        // verify that the file is zipped
        verify(mockZipHelper).zipWithPassword(ImmutableList.of(csvFile), zipFile, PASSWORD);

        // verify that the email with attachment is sent
        verify(mockSesHelper).sendEmailWithAttachmentToAccount(appInfo, accountInfo, zipFile.getAbsolutePath());

        // cleanup files
        processor.cleanupFiles(csvFile, zipFile, tmpDir);
    }

    // We do this instead of using a fixed EXPECTED_CSV_CONTENT so that we don't have to re-write these tests every
    // time StudyParticipant changes.
    private void assertCsvContent(String csvContent) throws IOException {
        // Parse the CSV. 1 header row, 3 data rows.
        List<String[]> csvLines;
        try (StringReader stringReader = new StringReader(csvContent);
                CSVReader csvReader = new CSVReader(stringReader)) {
            csvLines = csvReader.readAll();
        }
        assertEquals(csvLines.size(), 4);

        // Headers should match.
        String[] headers = processor.getCsvHeaders();
        assertEquals(csvLines.get(0), headers);

        // Row 1.
        Map<String, String> row1Map = parseCsvRowIntoMap(headers, csvLines.get(1));
        assertEquals(row1Map.get("firstName"), "test-first-name1");
        assertEquals(row1Map.get("lastName"), "test-last-name");
        assertEquals(row1Map.get("id"), "test-id-12345");
        assertEquals(row1Map.get("notifyByEmail"), "true");
        assertEquals(row1Map.get("externalIds"),
                "{\"externalId1\":\"test-id-1\",\"externalId2\":\"test-id-2\",\"externalId3\":\"test-id-3\"}");

        // Row 2.
        Map<String, String> row2Map = parseCsvRowIntoMap(headers, csvLines.get(2));
        assertEquals(row2Map.get("firstName"), "test-first-name2");
        assertEquals(row2Map.get("lastName"), "test-last-name");
        assertEquals(row2Map.get("id"), "test-id-12345");
        assertEquals(row2Map.get("notifyByEmail"), "true");
        assertEquals(row2Map.get("externalIds"),
                "{\"externalId1\":\"test-id-1\",\"externalId2\":\"test-id-2\",\"externalId3\":\"test-id-3\"}");

        // Row 3.
        Map<String, String> row3Map = parseCsvRowIntoMap(headers, csvLines.get(3));
        assertEquals(row3Map.get("firstName"), "test-first-name3");
        assertEquals(row3Map.get("lastName"), "test-last-name");
        assertEquals(row3Map.get("id"), "test-id-12345");
        assertEquals(row3Map.get("notifyByEmail"), "true");
        assertEquals(row3Map.get("externalIds"),
                "{\"externalId1\":\"test-id-1\",\"externalId2\":\"test-id-2\",\"externalId3\":\"test-id-3\"}");
    }

    private static Map<String, String> parseCsvRowIntoMap(String[] headers, String[] row) {
        assertEquals(row.length, headers.length);
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
            map.put(headers[i], row[i]);
        }
        return map;
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