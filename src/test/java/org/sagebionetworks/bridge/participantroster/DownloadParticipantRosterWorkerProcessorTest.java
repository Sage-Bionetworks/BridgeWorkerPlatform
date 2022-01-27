package org.sagebionetworks.bridge.participantroster;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.joda.time.DateTime;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.s3.S3Helper;
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
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.sagebionetworks.bridge.participantroster.DownloadParticipantRosterWorkerProcessor.CONFIG_KEY_PARTICIPANTROSTER_BUCKET;
import static org.testng.Assert.assertEquals;

public class DownloadParticipantRosterWorkerProcessorTest extends Mockito {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final String APP_ID = "test-app";
    private static final String USER_ID = "test-user-id";
    private static final String PASSWORD = "test-password";
    private static final String ORG_ID = "test-org-id";
    private static final String STUDY_ID = "test-study-id";
    private static final int PAGE_SIZE = 100;
    private static final DateTime NOW = DateTime.now();
    private static final String S3_FILE_NAME = APP_ID + "/" + STUDY_ID + "/ABC/user_data.zip";
    private static final String DOWNLOAD_URL = "https://s3/" + S3_FILE_NAME;

    @Mock
    private DynamoHelper mockDynamoHelper;
    @Mock
    private BridgeHelper mockBridgeHelper;
    @Mock
    private SesHelper mockSesHelper;
    @Mock
    private ZipHelper mockZipHelper;
    @Mock
    private S3Helper mockS3Helper;
    @Mock
    private Config mockConfig;
    // mocked manually
    private InMemoryFileHelper fileHelper;
    @Captor
    ArgumentCaptor<ObjectMetadata> metadataCaptor;
    @Spy
    @InjectMocks
    private DownloadParticipantRosterWorkerProcessor processor;

    @BeforeMethod
    public void before() {
        MockitoAnnotations.initMocks(this);
        fileHelper = new InMemoryFileHelper();
        processor.setFileHelper(fileHelper);
        processor.setPageSize(PAGE_SIZE);
        
        when(mockConfig.get(CONFIG_KEY_PARTICIPANTROSTER_BUCKET)).thenReturn("participant-roster-bucket");
        processor.setBridgeConfig(mockConfig);
        
        when(processor.getNextToken()).thenReturn("ABC");
        when(processor.getDateTime()).thenReturn(NOW);
    }

    @Test
    public void notResearcherOrCoordinator() throws Exception {
        // initialize participant with no Researcher or Study Coordinator role
        StudyParticipant participant = new StudyParticipant();
        participant.setRoles(ImmutableList.of());
        participant.setEmail("example@example.org");
        participant.setEmailVerified(true);
        
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
        participant.addRolesItem(Role.STUDY_COORDINATOR);
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
    public void processWithResearcher() throws Exception {
        // set up participant
        StudyParticipant participant = new StudyParticipant();
        participant.setEmail("example@example.org");
        participant.setEmailVerified(true);
        participant.addRolesItem(Role.RESEARCHER);

        // set up files
        File tmpDir = fileHelper.createTempDir();
        File csvFile = fileHelper.newFile(tmpDir, "participant_roster.csv");
        File zipFile = fileHelper.newFile(tmpDir, "user_data.zip");

        // set up request
        DownloadParticipantRosterRequest request = new DownloadParticipantRosterRequest.Builder()
                .withUserId(USER_ID).withAppId(APP_ID).withPassword(PASSWORD).withStudyId(STUDY_ID).build();

        // set up mocks
        doReturn(participant).when(mockBridgeHelper).getParticipant(APP_ID, USER_ID, false);

        Study study = new Study();
        study.setIdentifier(STUDY_ID);
        List<Study> studies = ImmutableList.of(study);
        doReturn(studies).when(mockBridgeHelper).getSponsoredStudiesForApp(APP_ID, ORG_ID, 0, PAGE_SIZE);
        
        when(mockS3Helper.generatePresignedUrl("participant-roster-bucket", S3_FILE_NAME, NOW.plusDays(3),
                HttpMethod.GET)).thenReturn(new URL(DOWNLOAD_URL));

        List<StudyParticipant> studyParticipants = ImmutableList.of(makeStudyParticipant(1), makeStudyParticipant(2), makeStudyParticipant(3));
        doReturn(studyParticipants).when(mockBridgeHelper).getStudyParticipantsForStudy(APP_ID, STUDY_ID, 0, PAGE_SIZE);
        doReturn(ImmutableList.of()).when(mockBridgeHelper).getStudyParticipantsForStudy(APP_ID, STUDY_ID, PAGE_SIZE, PAGE_SIZE);

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
        verify(processor).writeStudyParticipants(any(CSVWriter.class), eq(studyParticipants), eq(0), eq(APP_ID), eq(STUDY_ID));

        // verify that the file is zipped
        verify(mockZipHelper).zipWithPassword(ImmutableList.of(csvFile), zipFile, PASSWORD);

        // verify that the email with attachment is sent
        verify(mockSesHelper).sendEmailWithTempDownloadLinkToAccount(appInfo, accountInfo, DOWNLOAD_URL, "3 days");

        // cleanup files
        processor.cleanupFiles(csvFile, zipFile, tmpDir);
    }

    @Test
    public void processWithStudyCoordinator() throws Exception {
        // set up participant
        StudyParticipant participant = new StudyParticipant();
        participant.setEmail("example@example.org");
        participant.setEmailVerified(true);
        participant.addRolesItem(Role.STUDY_COORDINATOR);
        participant.setOrgMembership(ORG_ID);

        // set up files
        File tmpDir = fileHelper.createTempDir();
        File csvFile = fileHelper.newFile(tmpDir, "participant_roster.csv");
        File zipFile = fileHelper.newFile(tmpDir, "user_data.zip");

        // set up request
        DownloadParticipantRosterRequest request = new DownloadParticipantRosterRequest.Builder()
                .withUserId(USER_ID).withAppId(APP_ID).withPassword(PASSWORD).withStudyId(STUDY_ID).build();

        // set up mocks
        doReturn(participant).when(mockBridgeHelper).getParticipant(APP_ID, USER_ID, false);

        // the original implementation did not look past the first page of records. Put the relevant
        // study on the second page to verify this is found.
        processor.setPageSize(1);
        
        Study study1 = new Study();
        study1.setIdentifier("some-other-study");
        doReturn(ImmutableList.of(study1)).when(mockBridgeHelper)
            .getSponsoredStudiesForApp(APP_ID, ORG_ID, 0, 1);
        
        Study study2 = new Study();
        study2.setIdentifier(STUDY_ID);
        doReturn(ImmutableList.of(study2)).when(mockBridgeHelper)
            .getSponsoredStudiesForApp(APP_ID, ORG_ID, 1, 1);
        
        when(mockS3Helper.generatePresignedUrl("participant-roster-bucket", S3_FILE_NAME, NOW.plusDays(3),
                HttpMethod.GET)).thenReturn(new URL(DOWNLOAD_URL));

        List<StudyParticipant> studyParticipants = ImmutableList.of(makeStudyParticipant(1), makeStudyParticipant(2), makeStudyParticipant(3));
        doReturn(studyParticipants).when(mockBridgeHelper).getStudyParticipantsForStudy(APP_ID, STUDY_ID, 0, 1);
        doReturn(ImmutableList.of()).when(mockBridgeHelper).getStudyParticipantsForStudy(APP_ID, STUDY_ID, 1, 1);

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
        verify(processor).writeStudyParticipants(any(CSVWriter.class), eq(studyParticipants), eq(0), eq(APP_ID), eq(STUDY_ID));

        // verify that the file is zipped
        verify(mockZipHelper).zipWithPassword(ImmutableList.of(csvFile), zipFile, PASSWORD);

        // verify that the email with attachment is sent
        verify(mockSesHelper).sendEmailWithTempDownloadLinkToAccount(appInfo, accountInfo, DOWNLOAD_URL, "3 days");

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
        DownloadParticipantRosterRequest request = new DownloadParticipantRosterRequest.Builder().withUserId(USER_ID)
                .withAppId(APP_ID).withStudyId(STUDY_ID).withPassword(PASSWORD).build();

        // mock caller
        doReturn(participant).when(mockBridgeHelper).getParticipant(APP_ID, USER_ID, false);
        
        doReturn(new URL(DOWNLOAD_URL)).when(mockS3Helper).generatePresignedUrl(
                "participant-roster-bucket", S3_FILE_NAME, NOW.plusDays(3), HttpMethod.GET);

        // mock multiple bridge calls to test pagination
        StudyParticipant participant1 = makeStudyParticipant(1);
        StudyParticipant participant2 = makeStudyParticipant(2);
        StudyParticipant participant3 = makeStudyParticipant(3);

        doReturn(ImmutableList.of(participant1)).when(mockBridgeHelper).getStudyParticipantsForStudy(APP_ID, STUDY_ID, 0, 1);
        doReturn(ImmutableList.of(participant2)).when(mockBridgeHelper).getStudyParticipantsForStudy(APP_ID, STUDY_ID, 1, 1);
        doReturn(ImmutableList.of(participant3)).when(mockBridgeHelper).getStudyParticipantsForStudy(APP_ID, STUDY_ID, 2, 1);

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
                eq(APP_ID), eq(STUDY_ID));

        verify(mockS3Helper).writeFileToS3(eq("participant-roster-bucket"), eq(S3_FILE_NAME), eq(zipFile),
                metadataCaptor.capture());
        
        assertEquals(metadataCaptor.getValue().getSSEAlgorithm(), ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        assertEquals(metadataCaptor.getValue().getContentType(), "application/zip");
        assertEquals(metadataCaptor.getValue().getContentDisposition(), "attachment; filename=\"user_data.zip\"");
        
        // verify that the file is zipped
        verify(mockZipHelper).zipWithPassword(ImmutableList.of(csvFile), zipFile, PASSWORD);

        // verify that the email with attachment is sent
        verify(mockSesHelper).sendEmailWithTempDownloadLinkToAccount(appInfo, accountInfo, DOWNLOAD_URL, "3 days");

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
        requestNode.put("studyId", STUDY_ID);

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
        doReturn(new ArrayList<StudyParticipant>()).when(mockBridgeHelper).getStudyParticipantsForStudy(
                anyString(), anyString(), anyInt(), anyInt());

        processor.accept(requestNode);

        // verify that this call isn't reached, because study participant does not have a verified email
        verify(mockBridgeHelper, never()).getStudyParticipantsForStudy(anyString(), anyString(), anyInt(), anyInt());
    }
}