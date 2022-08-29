package org.sagebionetworks.bridge.exporter3;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.sagebionetworks.repo.model.table.ColumnModel;
import org.sagebionetworks.repo.model.table.PartialRow;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.synapse.SynapseHelper;

public class ParticipantVersionHelperTest {
    private static final List<String> DATA_GROUPS = ImmutableList.of("bbb-group", "aaa-group");
    private static final String HEALTH_CODE = "health-code";
    private static final List<String> LANGUAGES = ImmutableList.of("es-ES", "en-UK");
    private static final int PARTICIPANT_VERSION = 42;
    private static final String PARTICIPANT_VERSION_TABLE_ID_FOR_APP = "syn11111";
    private static final String PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY = "syn22222";
    private static final Map<String, String> STUDY_MEMBERSHIPS = ImmutableMap.of("studyC", ParticipantVersionHelper.EXT_ID_NONE,
            "studyB", "extB", "studyA", "extA");
    private static final String TIME_ZONE = "America/Los_Angeles";

    private static final DateTime CREATED_ON = DateTime.parse("2022-01-14T01:19:21.201-0800");
    private static final long CREATED_ON_MILLIS = CREATED_ON.getMillis();

    private static final DateTime MODIFIED_ON = DateTime.parse("2022-01-14T13:08:28.583-0800");
    private static final long MODIFIED_ON_MILLIS = MODIFIED_ON.getMillis();

    private static final String COLUMN_ID_HEALTH_CODE = "healthCode-col-id";
    private static final String COLUMN_ID_PARTICIPANT_VERSION = "participantVersion-col-id";
    private static final String COLUMN_ID_CREATED_ON = "createdOn-col-id";
    private static final String COLUMN_ID_MODIFIED_ON = "modifiedOn-col-id";
    private static final String COLUMN_ID_DATA_GROUPS = "dataGroups-col-id";
    private static final String COLUMN_ID_LANGUAGES = "languages-col-id";
    private static final String COLUMN_ID_SHARING_SCOPE = "sharingScope-col-id";
    private static final String COLUMN_ID_STUDY_MEMBERSHIPS = "studyMemberships-col-id";
    private static final String COLUMN_ID_CLIENT_TIME_ZONE = "clientTimeZone-col-id";

    // We only care about column name and ID.
    private static final List<ColumnModel> COLUMN_MODEL_LIST;
    static {
        COLUMN_MODEL_LIST = new ImmutableList.Builder<ColumnModel>()
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_HEALTH_CODE)
                        .setId(COLUMN_ID_HEALTH_CODE))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_PARTICIPANT_VERSION)
                        .setId(COLUMN_ID_PARTICIPANT_VERSION))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_CREATED_ON)
                        .setId(COLUMN_ID_CREATED_ON))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_MODIFIED_ON)
                        .setId(COLUMN_ID_MODIFIED_ON))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_DATA_GROUPS)
                        .setId(COLUMN_ID_DATA_GROUPS))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_LANGUAGES)
                        .setId(COLUMN_ID_LANGUAGES))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_SHARING_SCOPE)
                        .setId(COLUMN_ID_SHARING_SCOPE))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_STUDY_MEMBERSHIPS)
                        .setId(COLUMN_ID_STUDY_MEMBERSHIPS))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_CLIENT_TIME_ZONE)
                        .setId(COLUMN_ID_CLIENT_TIME_ZONE))
                .build();
    }

    @Mock
    private SynapseHelper mockSynapseHelper;

    @InjectMocks
    private ParticipantVersionHelper participantVersionHelper;

    @BeforeMethod
    public void beforeMethod() throws Exception {
        MockitoAnnotations.initMocks(this);

        // Mock shared dependencies.
        when(mockSynapseHelper.getColumnModelsForTableWithRetry(any())).thenReturn(COLUMN_MODEL_LIST);
    }

    @Test
    public void makeRowWithoutStudy() throws Exception {
        // Execute.
        PartialRow row = participantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, makeParticipantVersion());

        // Validate.
        Map<String, String> rowValueMap = row.getValues();
        assertEquals(rowValueMap.size(), 9);
        assertEquals(rowValueMap.get(COLUMN_ID_HEALTH_CODE), HEALTH_CODE);
        assertEquals(rowValueMap.get(COLUMN_ID_PARTICIPANT_VERSION), String.valueOf(PARTICIPANT_VERSION));
        assertEquals(rowValueMap.get(COLUMN_ID_CREATED_ON), String.valueOf(CREATED_ON_MILLIS));
        assertEquals(rowValueMap.get(COLUMN_ID_MODIFIED_ON), String.valueOf(MODIFIED_ON_MILLIS));
        assertEquals(rowValueMap.get(COLUMN_ID_SHARING_SCOPE), SharingScope.SPONSORS_AND_PARTNERS.getValue());
        assertEquals(rowValueMap.get(COLUMN_ID_STUDY_MEMBERSHIPS), "|studyA=extA|studyB=extB|studyC=|");
        assertEquals(rowValueMap.get(COLUMN_ID_CLIENT_TIME_ZONE), TIME_ZONE);

        // Data groups is sorted alphabetically.
        assertEquals(rowValueMap.get(COLUMN_ID_DATA_GROUPS), "aaa-group,bbb-group");

        // Languages is not sorted, and the data format is a JSON array.
        JsonNode languagesNode = DefaultObjectMapper.INSTANCE.readTree(rowValueMap.get(COLUMN_ID_LANGUAGES));
        assertTrue(languagesNode.isArray());
        assertEquals(languagesNode.size(), 2);
        assertEquals(languagesNode.get(0).textValue(), "es-ES");
        assertEquals(languagesNode.get(1).textValue(), "en-UK");
    }

    @Test
    public void makeRowWithStudy() throws Exception {
        // Execute.
        PartialRow row = participantVersionHelper.makeRowForParticipantVersion("studyA",
                PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY, makeParticipantVersion());

        // Validate. The main difference is we filter out other studies in the Study Membership field.
        Map<String, String> rowValueMap = row.getValues();
        assertEquals(rowValueMap.get(COLUMN_ID_STUDY_MEMBERSHIPS), "|studyA=extA|");
    }

    @Test
    public void tooManyLanguages() throws Exception {
        // Max is 10. Make 11 fake languages of the form fake#.
        List<String> languageList = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            languageList.add("fake" + i);
        }

        ParticipantVersion participantVersion = makeParticipantVersion();
        participantVersion.setLanguages(languageList);

        // Execute.
        PartialRow row = participantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, participantVersion);

        // Just validate languages was truncated.
        Map<String, String> rowValueMap = row.getValues();
        JsonNode languagesNode = DefaultObjectMapper.INSTANCE.readTree(rowValueMap.get(COLUMN_ID_LANGUAGES));
        assertTrue(languagesNode.isArray());
        assertEquals(languagesNode.size(), 10);
        for (int i = 0; i < languagesNode.size(); i++) {
            assertEquals(languagesNode.get(i).textValue(), "fake" + i);
        }
    }

    @Test
    public void languageTooLong() throws Exception {
        // Max language string length is 5.
        List<String> languageList = new ArrayList<>();
        languageList.add("language");

        ParticipantVersion participantVersion = makeParticipantVersion();
        participantVersion.setLanguages(languageList);

        // Execute.
        PartialRow row = participantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, participantVersion);

        // Just validate language string was truncated.
        Map<String, String> rowValueMap = row.getValues();
        JsonNode languagesNode = DefaultObjectMapper.INSTANCE.readTree(rowValueMap.get(COLUMN_ID_LANGUAGES));
        assertTrue(languagesNode.isArray());
        assertEquals(languagesNode.size(), 1);
        assertEquals(languagesNode.get(0).textValue(), "langu");
    }

    // branch coverage
    @Test
    public void emptyParticipantVersion() throws Exception {
        // Execute.
        PartialRow row = participantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, new ParticipantVersion());

        // Validate.
        Map<String, String> rowValueMap = row.getValues();
        assertTrue(rowValueMap.isEmpty());
    }

    // branch coverage
    @Test
    public void emptySubstudyMemberships() throws Exception {
        ParticipantVersion participantVersion = makeParticipantVersion();
        participantVersion.setStudyMemberships(ImmutableMap.of());

        // Execute.
        PartialRow row = participantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, participantVersion);

        // Validate. Mostly just validate that something was submitted and that studyMemberships isn't in the map.
        Map<String, String> rowValueMap = row.getValues();
        assertFalse(rowValueMap.isEmpty());
        assertFalse(rowValueMap.containsKey(COLUMN_ID_STUDY_MEMBERSHIPS));
    }

    private static ParticipantVersion makeParticipantVersion() {
        ParticipantVersion participantVersion = new ParticipantVersion();
        participantVersion.setAppId(Exporter3TestUtil.APP_ID);
        participantVersion.setHealthCode(HEALTH_CODE);
        participantVersion.setParticipantVersion(PARTICIPANT_VERSION);
        participantVersion.setCreatedOn(CREATED_ON);
        participantVersion.setModifiedOn(MODIFIED_ON);
        participantVersion.setDataGroups(DATA_GROUPS);
        participantVersion.setLanguages(LANGUAGES);
        participantVersion.setSharingScope(SharingScope.SPONSORS_AND_PARTNERS);
        participantVersion.setStudyMemberships(STUDY_MEMBERSHIPS);
        participantVersion.setTimeZone(TIME_ZONE);
        return participantVersion;
    }
}
