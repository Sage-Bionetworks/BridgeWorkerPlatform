package org.sagebionetworks.bridge.exporter3;

import static org.mockito.AdditionalMatchers.or;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import org.sagebionetworks.bridge.rest.model.DemographicResponse;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.client.exceptions.SynapseException;

public class ParticipantVersionHelperTest {
    private static final List<String> DATA_GROUPS = ImmutableList.of("bbb-group", "aaa-group");
    private static final String HEALTH_CODE = "health-code";
    private static final List<String> LANGUAGES = ImmutableList.of("es-ES", "en-UK");
    private static final int PARTICIPANT_VERSION = 42;
    private static final String PARTICIPANT_VERSION_TABLE_ID_FOR_APP = "syn11111";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP = "syn22222";
    private static final String PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY = "syn33333";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY = "syn44444";
    private static final Map<String, String> STUDY_MEMBERSHIPS = ImmutableMap.of("studyC", ParticipantVersionHelper.EXT_ID_NONE,
            "studyB", "extB", "studyA", "extA");
    private static final String TIME_ZONE = "America/Los_Angeles";

    private static final DateTime CREATED_ON = DateTime.parse("2022-01-14T01:19:21.201-0800");
    private static final long CREATED_ON_MILLIS = CREATED_ON.getMillis();

    private static final DateTime MODIFIED_ON = DateTime.parse("2022-01-14T13:08:28.583-0800");
    private static final long MODIFIED_ON_MILLIS = MODIFIED_ON.getMillis();

    private static final String PARTICIPANT_VERSION_COLUMN_ID_HEALTH_CODE = "participantVersion-healthCode-col-id";
    private static final String PARTICIPANT_VERSION_COLUMN_ID_PARTICIPANT_VERSION = "participantVersion-participantVersion-col-id";
    private static final String PARTICIPANT_VERSION_COLUMN_ID_CREATED_ON = "participantVersion-createdOn-col-id";
    private static final String PARTICIPANT_VERSION_COLUMN_ID_MODIFIED_ON = "participantVersion-modifiedOn-col-id";
    private static final String PARTICIPANT_VERSION_COLUMN_ID_DATA_GROUPS = "participantVersion-dataGroups-col-id";
    private static final String PARTICIPANT_VERSION_COLUMN_ID_LANGUAGES = "participantVersion-languages-col-id";
    private static final String PARTICIPANT_VERSION_COLUMN_ID_SHARING_SCOPE = "participantVersion-sharingScope-col-id";
    private static final String PARTICIPANT_VERSION_COLUMN_ID_STUDY_MEMBERSHIPS = "participantVersion-studyMemberships-col-id";
    private static final String PARTICIPANT_VERSION_COLUMN_ID_CLIENT_TIME_ZONE = "participantVersion-clientTimeZone-col-id";

    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_HEALTH_CODE = "participantVersionDemographics-healthCode-col-id";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_PARTICIPANT_VERSION = "participantVersionDemographics-participantVersion-col-id";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_STUDY_ID = "participantVersionDemographics-studyId-col-id";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_CATEGORY_NAME = "participantVersionDemographics-categoryName-col-id";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_VALUE = "participantVersionDemographics-value-col-id";
    private static final String PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_UNITS = "participantVersionDemographics-units-col-id";

    private static final Map<String, DemographicResponse> APP_DEMOGRAPHICS;
    static {
        APP_DEMOGRAPHICS = new LinkedHashMap<>();
        APP_DEMOGRAPHICS.put("category1",
                new DemographicResponse().multipleSelect(false).values(ImmutableList.of("foo")).units("units"));
        APP_DEMOGRAPHICS.put("category2",
                new DemographicResponse().multipleSelect(true).values(ImmutableList.of("1", "-5.7")).units(null));
        APP_DEMOGRAPHICS.put(null,
                new DemographicResponse().multipleSelect(true).values(ImmutableList.of("bar")).units("units2"));
        APP_DEMOGRAPHICS.put("category3", null);
    }
    private static final List<PartialRow> EXPECTED_APP_ROWS;
    static {
        EXPECTED_APP_ROWS = new ArrayList<>();

        Map<String, String> row1Map = new HashMap<>();
        row1Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_HEALTH_CODE, HEALTH_CODE);
        row1Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_PARTICIPANT_VERSION,
                String.valueOf(PARTICIPANT_VERSION));
        row1Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_STUDY_ID, null);
        row1Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_CATEGORY_NAME, "category1");
        row1Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_VALUE, "foo");
        row1Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_UNITS, "units");
        EXPECTED_APP_ROWS.add(new PartialRow().setValues(row1Map));

        Map<String, String> row2Map = new HashMap<>();
        row2Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_HEALTH_CODE, HEALTH_CODE);
        row2Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_PARTICIPANT_VERSION,
                String.valueOf(PARTICIPANT_VERSION));
        row2Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_STUDY_ID, null);
        row2Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_CATEGORY_NAME, "category2");
        row2Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_VALUE, "1");
        EXPECTED_APP_ROWS.add(new PartialRow().setValues(row2Map));

        Map<String, String> row3Map = new HashMap<>();
        row3Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_HEALTH_CODE, HEALTH_CODE);
        row3Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_PARTICIPANT_VERSION,
                String.valueOf(PARTICIPANT_VERSION));
        row3Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_STUDY_ID, null);
        row3Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_CATEGORY_NAME, "category2");
        row3Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_VALUE, "-5.7");
        EXPECTED_APP_ROWS.add(new PartialRow().setValues(row3Map));
    }
    private static final String STUDY_ID = "study";
    private static final String STUDY2_ID = "study2";
    private static final String STUDY_NULL_ID = "study null";
    private static final String STUDY_EMPTY_ID = "study empty";
    private static final Map<String, Map<String, DemographicResponse>> STUDY_DEMOGRAPHICS;
    static {
        STUDY_DEMOGRAPHICS = new HashMap<>();

        Map<String, DemographicResponse> STUDY1_DEMOGRAPHICS_MAP = new LinkedHashMap<>();
        STUDY1_DEMOGRAPHICS_MAP.put("category1",
                new DemographicResponse().multipleSelect(false).values(ImmutableList.of("foo")).units("units"));
        STUDY1_DEMOGRAPHICS_MAP.put("category2",
                new DemographicResponse().multipleSelect(true).values(ImmutableList.of("1", "-5.7")).units(null));
        STUDY1_DEMOGRAPHICS_MAP.put("category3",
                new DemographicResponse().multipleSelect(true).values(ImmutableList.of()).units("units2"));
        STUDY1_DEMOGRAPHICS_MAP.put(null,
                new DemographicResponse().multipleSelect(true).values(ImmutableList.of("bar")).units("units2"));
        STUDY1_DEMOGRAPHICS_MAP.put("category4", null);
        STUDY_DEMOGRAPHICS.put(STUDY_ID, STUDY1_DEMOGRAPHICS_MAP);

        // to test whether study demographics leak into other tables
        STUDY_DEMOGRAPHICS.put(STUDY2_ID, ImmutableMap.of(
                "category4",
                new DemographicResponse().multipleSelect(true).values(ImmutableList.of("bar", "baz")).units("units3")));

        STUDY_DEMOGRAPHICS.put(STUDY_NULL_ID, null);

        STUDY_DEMOGRAPHICS.put(STUDY_EMPTY_ID, ImmutableMap.of());
    }
    private static final List<PartialRow> EXPECTED_STUDY_ROWS;
    static {
        EXPECTED_STUDY_ROWS = new ArrayList<>();

        Map<String, String> row1Map = new HashMap<>();
        row1Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_HEALTH_CODE, HEALTH_CODE);
        row1Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_PARTICIPANT_VERSION,
                String.valueOf(PARTICIPANT_VERSION));
        row1Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_STUDY_ID, STUDY_ID);
        row1Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_CATEGORY_NAME, "category1");
        row1Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_VALUE, "foo");
        row1Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_UNITS, "units");
        EXPECTED_STUDY_ROWS.add(new PartialRow().setValues(row1Map));

        Map<String, String> row2Map = new HashMap<>();
        row2Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_HEALTH_CODE, HEALTH_CODE);
        row2Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_PARTICIPANT_VERSION,
                String.valueOf(PARTICIPANT_VERSION));
        row2Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_STUDY_ID, STUDY_ID);
        row2Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_CATEGORY_NAME, "category2");
        row2Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_VALUE, "1");
        EXPECTED_STUDY_ROWS.add(new PartialRow().setValues(row2Map));

        Map<String, String> row3Map = new HashMap<>();
        row3Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_HEALTH_CODE, HEALTH_CODE);
        row3Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_PARTICIPANT_VERSION,
                String.valueOf(PARTICIPANT_VERSION));
        row3Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_STUDY_ID, STUDY_ID);
        row3Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_CATEGORY_NAME, "category2");
        row3Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_VALUE, "-5.7");
        EXPECTED_STUDY_ROWS.add(new PartialRow().setValues(row3Map));

        Map<String, String> row4Map = new HashMap<>();
        row4Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_HEALTH_CODE, HEALTH_CODE);
        row4Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_PARTICIPANT_VERSION,
                String.valueOf(PARTICIPANT_VERSION));
        row4Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_STUDY_ID, STUDY_ID);
        row4Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_CATEGORY_NAME, "category3");
        row4Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_VALUE, null);
        row4Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_UNITS, "units2");
        EXPECTED_STUDY_ROWS.add(new PartialRow().setValues(row4Map));
    }
    private static final List<PartialRow> EXPECTED_STUDY2_ROWS;
    static {
        EXPECTED_STUDY2_ROWS = new ArrayList<>();

        Map<String, String> row1Map = new HashMap<>();
        row1Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_HEALTH_CODE, HEALTH_CODE);
        row1Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_PARTICIPANT_VERSION,
                String.valueOf(PARTICIPANT_VERSION));
        row1Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_STUDY_ID, STUDY2_ID);
        row1Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_CATEGORY_NAME, "category4");
        row1Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_VALUE, "bar");
        row1Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_UNITS, "units3");
        EXPECTED_STUDY2_ROWS.add(new PartialRow().setValues(row1Map));

        Map<String, String> row2Map = new HashMap<>();
        row2Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_HEALTH_CODE, HEALTH_CODE);
        row2Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_PARTICIPANT_VERSION,
                String.valueOf(PARTICIPANT_VERSION));
        row2Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_STUDY_ID, STUDY2_ID);
        row2Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_CATEGORY_NAME, "category4");
        row2Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_VALUE, "baz");
        row2Map.put(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_UNITS, "units3");
        EXPECTED_STUDY2_ROWS.add(new PartialRow().setValues(row2Map));
    }

    // We only care about column name and ID.
    private static final List<ColumnModel> PARTICIPANT_VERSION_COLUMN_MODEL_LIST;
    static {
        PARTICIPANT_VERSION_COLUMN_MODEL_LIST = new ImmutableList.Builder<ColumnModel>()
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_HEALTH_CODE)
                        .setId(PARTICIPANT_VERSION_COLUMN_ID_HEALTH_CODE))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_PARTICIPANT_VERSION)
                        .setId(PARTICIPANT_VERSION_COLUMN_ID_PARTICIPANT_VERSION))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_CREATED_ON)
                        .setId(PARTICIPANT_VERSION_COLUMN_ID_CREATED_ON))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_MODIFIED_ON)
                        .setId(PARTICIPANT_VERSION_COLUMN_ID_MODIFIED_ON))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_DATA_GROUPS)
                        .setId(PARTICIPANT_VERSION_COLUMN_ID_DATA_GROUPS))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_LANGUAGES)
                        .setId(PARTICIPANT_VERSION_COLUMN_ID_LANGUAGES))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_SHARING_SCOPE)
                        .setId(PARTICIPANT_VERSION_COLUMN_ID_SHARING_SCOPE))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_STUDY_MEMBERSHIPS)
                        .setId(PARTICIPANT_VERSION_COLUMN_ID_STUDY_MEMBERSHIPS))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_CLIENT_TIME_ZONE)
                        .setId(PARTICIPANT_VERSION_COLUMN_ID_CLIENT_TIME_ZONE))
                .build();
    }
    private static final List<ColumnModel> PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_MODEL_LIST;
    static {
        PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_MODEL_LIST = new ImmutableList.Builder<ColumnModel>()
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_HEALTH_CODE)
                        .setId(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_HEALTH_CODE))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_PARTICIPANT_VERSION)
                        .setId(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_PARTICIPANT_VERSION))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_STUDY_ID)
                        .setId(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_STUDY_ID))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_DEMOGRAPHIC_CATEGORY_NAME)
                        .setId(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_CATEGORY_NAME))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_DEMOGRAPHIC_VALUE)
                        .setId(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_VALUE))
                .add(new ColumnModel().setName(ParticipantVersionHelper.COLUMN_NAME_DEMOGRAPHIC_UNITS)
                        .setId(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_ID_UNITS))
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
        when(mockSynapseHelper.getColumnModelsForTableWithRetry(
                or(eq(PARTICIPANT_VERSION_TABLE_ID_FOR_APP),
                        eq(PARTICIPANT_VERSION_TABLE_ID_FOR_STUDY))))
                .thenReturn(PARTICIPANT_VERSION_COLUMN_MODEL_LIST);
        when(mockSynapseHelper.getColumnModelsForTableWithRetry(
                or(eq(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP),
                        eq(PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_STUDY))))
                .thenReturn(PARTICIPANT_VERSION_DEMOGRAPHICS_COLUMN_MODEL_LIST);
    }

    @Test
    public void makeRowWithoutStudy() throws Exception {
        // Execute.
        PartialRow row = participantVersionHelper.makeRowForParticipantVersion(null,
                PARTICIPANT_VERSION_TABLE_ID_FOR_APP, makeParticipantVersion());

        // Validate.
        Map<String, String> rowValueMap = row.getValues();
        assertEquals(rowValueMap.size(), 9);
        assertEquals(rowValueMap.get(PARTICIPANT_VERSION_COLUMN_ID_HEALTH_CODE), HEALTH_CODE);
        assertEquals(rowValueMap.get(PARTICIPANT_VERSION_COLUMN_ID_PARTICIPANT_VERSION), String.valueOf(PARTICIPANT_VERSION));
        assertEquals(rowValueMap.get(PARTICIPANT_VERSION_COLUMN_ID_CREATED_ON), String.valueOf(CREATED_ON_MILLIS));
        assertEquals(rowValueMap.get(PARTICIPANT_VERSION_COLUMN_ID_MODIFIED_ON), String.valueOf(MODIFIED_ON_MILLIS));
        assertEquals(rowValueMap.get(PARTICIPANT_VERSION_COLUMN_ID_SHARING_SCOPE), SharingScope.SPONSORS_AND_PARTNERS.getValue());
        assertEquals(rowValueMap.get(PARTICIPANT_VERSION_COLUMN_ID_STUDY_MEMBERSHIPS), "|studyA=extA|studyB=extB|studyC=|");
        assertEquals(rowValueMap.get(PARTICIPANT_VERSION_COLUMN_ID_CLIENT_TIME_ZONE), TIME_ZONE);

        // Data groups is sorted alphabetically.
        assertEquals(rowValueMap.get(PARTICIPANT_VERSION_COLUMN_ID_DATA_GROUPS), "aaa-group,bbb-group");

        // Languages is not sorted, and the data format is a JSON array.
        JsonNode languagesNode = DefaultObjectMapper.INSTANCE.readTree(rowValueMap.get(PARTICIPANT_VERSION_COLUMN_ID_LANGUAGES));
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
        assertEquals(rowValueMap.get(PARTICIPANT_VERSION_COLUMN_ID_STUDY_MEMBERSHIPS), "|studyA=extA|");
    }

    @Test
    public void makeDemographicsRowsForApp() throws SynapseException {
        List<PartialRow> rows = participantVersionHelper.makeRowsForParticipantVersionDemographics(null,
                PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP, makeParticipantVersion());

        List<PartialRow> expectedRows = new ArrayList<>();
        expectedRows.addAll(EXPECTED_APP_ROWS);
        expectedRows.addAll(EXPECTED_STUDY_ROWS);
        expectedRows.addAll(EXPECTED_STUDY2_ROWS);
        assertEquals(rows, expectedRows);
    }

    @Test
    public void makeDemographicsRowsForAppEmpty() throws SynapseException {
        ParticipantVersion participantVersion = makeParticipantVersion();
        participantVersion.setAppDemographics(ImmutableMap.of());
        List<PartialRow> rows = participantVersionHelper.makeRowsForParticipantVersionDemographics(null,
                PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP, participantVersion);

        List<PartialRow> expectedRows = new ArrayList<>();
        expectedRows.addAll(EXPECTED_STUDY_ROWS);
        expectedRows.addAll(EXPECTED_STUDY2_ROWS);
        assertEquals(rows, expectedRows);
    }

    @Test
    public void makeDemographicsRowsNullAppDemographicsAndStudyDemographics() throws SynapseException {
        ParticipantVersion participantVersion = makeParticipantVersion();
        participantVersion.setAppDemographics(null);
        participantVersion.setStudyDemographics(null);
        List<PartialRow> rows = participantVersionHelper.makeRowsForParticipantVersionDemographics(null,
                PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP, participantVersion);

        assertEquals(rows.size(), 0);
    }

    @Test
    public void makeDemographicsRowsForStudy() throws SynapseException {
        List<PartialRow> rows = participantVersionHelper.makeRowsForParticipantVersionDemographics(STUDY_ID,
                PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP, makeParticipantVersion());

        List<PartialRow> expectedRows = new ArrayList<>();
        expectedRows.addAll(EXPECTED_APP_ROWS);
        expectedRows.addAll(EXPECTED_STUDY_ROWS);
        assertEquals(rows, expectedRows);
    }

    @Test
    public void makeDemographicsRowsForStudyNullDemographics() throws SynapseException {
        List<PartialRow> rows = participantVersionHelper.makeRowsForParticipantVersionDemographics(STUDY_NULL_ID,
                PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP, makeParticipantVersion());

        assertEquals(rows, EXPECTED_APP_ROWS);
    }

    @Test
    public void makeDemographicsRowsForStudyEmptyDemographics() throws SynapseException {
        List<PartialRow> rows = participantVersionHelper.makeRowsForParticipantVersionDemographics(STUDY_EMPTY_ID,
                PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP, makeParticipantVersion());

        assertEquals(rows, EXPECTED_APP_ROWS);
    }

    @Test
    public void makeDemographicsRowsNullHealthCode() throws SynapseException {
        ParticipantVersion participantVersion = makeParticipantVersion();
        participantVersion.setHealthCode(null);
        List<PartialRow> rows = participantVersionHelper.makeRowsForParticipantVersionDemographics(STUDY_ID,
                PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP, participantVersion);

        assertEquals(rows.size(), 0);
    }

    @Test
    public void makeDemographicsRowsNullVersionNum() throws SynapseException {
        ParticipantVersion participantVersion = makeParticipantVersion();
        participantVersion.setParticipantVersion(null);
        List<PartialRow> rows = participantVersionHelper.makeRowsForParticipantVersionDemographics(STUDY_ID,
                PARTICIPANT_VERSION_DEMOGRAPHICS_TABLE_ID_FOR_APP, participantVersion);

        assertEquals(rows.size(), 0);
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
        JsonNode languagesNode = DefaultObjectMapper.INSTANCE.readTree(rowValueMap.get(PARTICIPANT_VERSION_COLUMN_ID_LANGUAGES));
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
        JsonNode languagesNode = DefaultObjectMapper.INSTANCE.readTree(rowValueMap.get(PARTICIPANT_VERSION_COLUMN_ID_LANGUAGES));
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
        assertFalse(rowValueMap.containsKey(PARTICIPANT_VERSION_COLUMN_ID_STUDY_MEMBERSHIPS));
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
        participantVersion.setAppDemographics(APP_DEMOGRAPHICS);
        participantVersion.setStudyDemographics(STUDY_DEMOGRAPHICS);
        return participantVersion;
    }
}
