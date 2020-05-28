package org.sagebionetworks.bridge.workerPlatform.dynamodb;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.notification.worker.NotificationType;
import org.sagebionetworks.bridge.notification.worker.UserNotification;
import org.sagebionetworks.bridge.notification.worker.WorkerConfig;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

public class DynamoHelperTest {
    private static final String DEFAULT_TABLE_ID = "default-table";
    private static final long MOCK_NOW_MILLIS = DateTime.parse("2018-04-27T16:41:15.831-0700").getMillis();
    private static final String APP_ID = "test-app";
    private static final String USER_ID = "test-user";

    private static final String DUMMY_FIELD_DEF_LIST_JSON = "[\n" +
            "   {\n" +
            "       \"name\":\"dummy-field\",\n" +
            "       \"type\":\"STRING\"\n" +
            "   }\n" +
            "]";

    private static final UploadSchemaKey TEST_SCHEMA_KEY = new UploadSchemaKey.Builder().withAppId(APP_ID)
            .withSchemaId("test-schema").withRevision(42).build();

    private DynamoHelper dynamoHelper;

    @Mock
    private DynamoQueryHelper mockQueryHelper;

    @Mock
    private Table mockNotificationConfigTable;

    @Mock
    private Table mockNotificationLogTable;

    @Mock
    private Index mockSchemaAppIndex;

    @Mock
    private Table mockSchemaTable;

    @Mock
    private Table mockAppTable;

    @Mock
    private Table mockSynapseMetaTable;

    @Mock
    private Table mockSynapseMapTable;

    @Mock
    private Table mockSynapseSurveyTable;

    @Mock
    private Table mockWorkerLogTable;

    @BeforeClass
    public static void mockNow() {
        DateTimeUtils.setCurrentMillisFixed(MOCK_NOW_MILLIS);
    }

    @AfterClass
    public static void unmockNow() {
        DateTimeUtils.setCurrentMillisSystem();
    }

    @BeforeMethod
    public void before() {
        // Set up mocks
        MockitoAnnotations.initMocks(this);

        // Create DynamoHelper
        dynamoHelper = new DynamoHelper();
        dynamoHelper.setDynamoQueryHelper(mockQueryHelper);
        dynamoHelper.setDdbNotificationConfigTable(mockNotificationConfigTable);
        dynamoHelper.setDdbNotificationLogTable(mockNotificationLogTable);
        dynamoHelper.setDdbAppTable(mockAppTable);
        dynamoHelper.setDdbSynapseMetaTable(mockSynapseMetaTable);
        dynamoHelper.setDdbSynapseMapTable(mockSynapseMapTable);
        dynamoHelper.setDdbSynapseSurveyTablesTable(mockSynapseSurveyTable);
        dynamoHelper.setDdbUploadSchemaAppIndex(mockSchemaAppIndex);
        dynamoHelper.setDdbUploadSchemaTable(mockSchemaTable);
        dynamoHelper.setDdbWorkerLogTable(mockWorkerLogTable);
    }

    @Test
    public void getDefaultSynapseTableForApp_SuccessCase() {
        // Mock SynapseMetaTables table.
        Item mockItem = new Item().withString(DynamoHelper.ATTR_TABLE_NAME, APP_ID + DynamoHelper.SUFFIX_DEFAULT)
                .withString(DynamoHelper.ATTR_TABLE_ID, DEFAULT_TABLE_ID);
        when(mockSynapseMetaTable.getItem(DynamoHelper.ATTR_TABLE_NAME,
                APP_ID + DynamoHelper.SUFFIX_DEFAULT)).thenReturn(mockItem);

        // Execute and validate.
        String retval = dynamoHelper.getDefaultSynapseTableForApp(APP_ID);
        assertEquals(retval, DEFAULT_TABLE_ID);
    }

    @Test
    public void getDefaultSynapseTableForApp_NoTable() {
        // Mock SynapseMetaTables table.
        when(mockSynapseMetaTable.getItem(DynamoHelper.ATTR_TABLE_NAME,
                APP_ID + DynamoHelper.SUFFIX_DEFAULT)).thenReturn(null);

        // Execute and validate.
        String retval = dynamoHelper.getDefaultSynapseTableForApp(APP_ID);
        assertNull(retval);
    }

    @Test
    public void deleteDefaultSynapseTableForApp() {
        // Execute and validate.
        dynamoHelper.deleteDefaultSynapseTableForApp(APP_ID);
        verify(mockSynapseMetaTable).deleteItem(DynamoHelper.ATTR_TABLE_NAME,
                APP_ID + DynamoHelper.SUFFIX_DEFAULT);
    }

    @Test
    public void getNotificationConfigForApp() {
        // Set up dummy maps
        List<String> missedCumulativeMessagesList = ImmutableList.of("cumulative-message-0", "cumulative-message-1",
                "cumulative-message-2");
        List<String> missedEarlyMessagesList = ImmutableList.of("early-message-0", "early-message-1",
                "early-message-2");
        List<String> missedLaterMessagesList = ImmutableList.of("later-message-0", "later-message-1",
                "later-message-2");
        Map<String, List<String>> preburstMessagesMap = ImmutableMap.of(
                "required-group-1", ImmutableList.of("preburst-message-1a", "preburst-message-1b"),
                "required-group-2", ImmutableList.of("preburst-message-2a", "preburst-message-2b"));

        // Set up mock
        Item item = new Item()
                .withPrimaryKey(DynamoHelper.KEY_APP_ID, APP_ID)
                .withString(DynamoHelper.KEY_APP_URL, "http://example.com/app-url")
                .withInt(DynamoHelper.KEY_BURST_DURATION_DAYS, 19)
                .withStringSet(DynamoHelper.KEY_BURST_EVENT_ID_SET, "enrollment", "custom:activityBurst2Start")
                .withString(DynamoHelper.KEY_BURST_TASK_ID, "study-burst-task")
                .withString(DynamoHelper.KEY_DEFAULT_PREBURST_MESSAGE, "default preburst message")
                .withInt(DynamoHelper.KEY_EARLY_LATE_CUTOFF_DAYS, 7)
                .withString(DynamoHelper.KEY_ENGAGEMENT_SURVEY_GUID, "dummy-survey-guid")
                .withStringSet(DynamoHelper.KEY_EXCLUDED_DATA_GROUP_SET, "excluded-group-1", "excluded-group-2")
                .withList(DynamoHelper.KEY_MISSED_CUMULATIVE_MESSAGES, missedCumulativeMessagesList)
                .withList(DynamoHelper.KEY_MISSED_EARLY_MESSAGES, missedEarlyMessagesList)
                .withList(DynamoHelper.KEY_MISSED_LATER_MESSAGES, missedLaterMessagesList)
                .withInt(DynamoHelper.KEY_NOTIFICATION_BLACKOUT_DAYS_FROM_START, 2)
                .withInt(DynamoHelper.KEY_NOTIFICATION_BLACKOUT_DAYS_FROM_END, 1)
                .withInt(DynamoHelper.KEY_NUM_ACTIVITIES_TO_COMPLETE, 6)
                .withInt(DynamoHelper.KEY_NUM_MISSED_CONSECUTIVE_DAYS_TO_NOTIFY, 3)
                .withInt(DynamoHelper.KEY_NUM_MISSED_DAYS_TO_NOTIFY, 4)
                .withMap(DynamoHelper.KEY_PREBURST_MESSAGES, preburstMessagesMap);
        when(mockNotificationConfigTable.getItem(DynamoHelper.KEY_APP_ID, APP_ID)).thenReturn(item);

        // Execute and validate
        WorkerConfig config = dynamoHelper.getNotificationConfigForApp(APP_ID);
        assertEquals(config.getAppUrl(), "http://example.com/app-url");
        assertEquals(config.getBurstDurationDays(), 19);
        assertEquals(config.getBurstStartEventIdSet(), ImmutableSet.of("enrollment",
                "custom:activityBurst2Start"));
        assertEquals(config.getBurstTaskId(), "study-burst-task");
        assertEquals(config.getDefaultPreburstMessage(), "default preburst message");
        assertEquals(config.getEarlyLateCutoffDays(), 7);
        assertEquals(config.getEngagementSurveyGuid(), "dummy-survey-guid");
        assertEquals(config.getExcludedDataGroupSet(), ImmutableSet.of("excluded-group-1",
                "excluded-group-2"));
        assertEquals(config.getMissedCumulativeActivitiesMessagesList(), missedCumulativeMessagesList);
        assertEquals(config.getMissedEarlyActivitiesMessagesList(), missedEarlyMessagesList);
        assertEquals(config.getMissedLaterActivitiesMessagesList(), missedLaterMessagesList);
        assertEquals(config.getNotificationBlackoutDaysFromStart(), 2);
        assertEquals(config.getNotificationBlackoutDaysFromEnd(), 1);
        assertEquals(config.getNumActivitiesToCompleteBurst(), 6);
        assertEquals(config.getNumMissedConsecutiveDaysToNotify(), 3);
        assertEquals(config.getNumMissedDaysToNotify(), 4);
        assertEquals(config.getPreburstMessagesByDataGroup(), preburstMessagesMap);

        verify(mockNotificationConfigTable).getItem(DynamoHelper.KEY_APP_ID, APP_ID);

        // NOTE: The caching is provided by the Spring container, and this helper is manually
        // constructed outside of the container. It should not exhibit any caching behavior.
        // Not sure how this test initially passed.
        // Test caching
        /*
        WorkerConfig config2 = dynamoHelper.getNotificationConfigForApp(APP_ID);
        assertNotNull(config2);
        verifyNoMoreInteractions(mockNotificationConfigTable);
        */
    }

    @Test
    public void getLastNotificationTimeForUser_NormalCase() {
        // Set up mock
        Item item = new Item().withPrimaryKey(DynamoHelper.KEY_USER_ID, USER_ID,

                DynamoHelper.KEY_NOTIFICATION_TIME, 1234L)
                .withString(DynamoHelper.KEY_MESSAGE, "dummy message")
                .withString(DynamoHelper.KEY_NOTIFICATION_TYPE, "LATE");
        when(mockQueryHelper.query(same(mockNotificationLogTable), any())).thenReturn(ImmutableList.of(item));

        // Execute and validate
        UserNotification result = dynamoHelper.getLastNotificationTimeForUser(USER_ID);
        assertEquals(result.getMessage(), "dummy message");
        assertEquals(result.getTime(), 1234L);
        assertEquals(result.getType(), NotificationType.LATE);
        assertEquals(result.getUserId(), USER_ID);

        ArgumentCaptor<QuerySpec> queryCaptor = ArgumentCaptor.forClass(QuerySpec.class);
        verify(mockQueryHelper).query(same(mockNotificationLogTable), queryCaptor.capture());

        QuerySpec query = queryCaptor.getValue();
        assertEquals(query.getHashKey().getName(), DynamoHelper.KEY_USER_ID);
        assertEquals(query.getHashKey().getValue(), USER_ID);
        assertFalse(query.isScanIndexForward());
        assertEquals(query.getMaxResultSize().intValue(), 1);
    }

    @Test
    public void getLastNotificationTimeForUser_NoNotificationType() {
        // Set up mock
        Item item = new Item().withPrimaryKey(DynamoHelper.KEY_USER_ID, USER_ID,
                DynamoHelper.KEY_NOTIFICATION_TIME, 1234L)
                .withString(DynamoHelper.KEY_MESSAGE, "dummy message");
        when(mockQueryHelper.query(same(mockNotificationLogTable), any())).thenReturn(ImmutableList.of(item));

        // Execute and validate
        UserNotification result = dynamoHelper.getLastNotificationTimeForUser(USER_ID);
        assertEquals(result.getType(), NotificationType.UNKNOWN);
    }

    @Test
    public void getLastNotificationTimeForUser_NoResult() {
        // Set up mock
        when(mockQueryHelper.query(same(mockNotificationLogTable), any())).thenReturn(ImmutableList.of());

        // Execute and validate
        UserNotification result = dynamoHelper.getLastNotificationTimeForUser(USER_ID);
        assertNull(result);
    }

    @Test
    public void setLastNotificationTimeForUser() {
        // Execute
        UserNotification userNotification = new UserNotification();
        userNotification.setMessage("dummy message");
        userNotification.setTime(1234L);
        userNotification.setType(NotificationType.LATE);
        userNotification.setUserId(USER_ID);
        dynamoHelper.setLastNotificationTimeForUser(userNotification);

        // Validate back-end
        ArgumentCaptor<Item> itemCaptor = ArgumentCaptor.forClass(Item.class);
        verify(mockNotificationLogTable).putItem(itemCaptor.capture());

        Item item = itemCaptor.getValue();
        assertEquals(item.getString(DynamoHelper.KEY_MESSAGE), "dummy message");
        assertEquals(item.getLong(DynamoHelper.KEY_NOTIFICATION_TIME), 1234L);
        assertEquals(item.getString(DynamoHelper.KEY_NOTIFICATION_TYPE), "LATE");
        assertEquals(item.getString(DynamoHelper.KEY_USER_ID), USER_ID);
    }

    @Test
    public void testGetApp() {
        // mock app table
        Item mockItem = new Item().withString("name", "Test App")
                .withString("shortName", "Test").withString("supportEmail", "support@sagebase.org");
        when(mockAppTable.getItem("identifier", "test-app")).thenReturn(mockItem);

        // execute and validate
        AppInfo appInfo = dynamoHelper.getApp("test-app");
        assertEquals(appInfo.getAppId(), "test-app");
        assertEquals(appInfo.getShortName(), "Test");
        assertEquals(appInfo.getName(), "Test App");
        assertEquals(appInfo.getSupportEmail(), "support@sagebase.org");
    }

    @Test
    public void testGetSynapseSurveyTables() {
        // mock Synapse survey table
        Item mockItem = new Item().withString("studyId", "test-app").withStringSet("tableIdSet", "foo-table",
                "bar-table");
        when(mockSynapseSurveyTable.getItem("studyId", "test-app")).thenReturn(mockItem);

        // execute and validate
        Set<String> tableIdSet = dynamoHelper.getSynapseSurveyTablesForApp("test-app");
        assertEquals(tableIdSet.size(), 2);
        assertTrue(tableIdSet.contains("foo-table"));
        assertTrue(tableIdSet.contains("bar-table"));
    }

    @Test
    public void testGetSynapseSurveyTablesNoTableIds() {
        // mock Synapse survey table
        Item mockItem = new Item().withString("studyId", "test-app");
        when(mockSynapseSurveyTable.getItem("studyId", "test-app")).thenReturn(mockItem);

        // execute and validate
        Set<String> tableIdSet = dynamoHelper.getSynapseSurveyTablesForApp("test-app");
        assertTrue(tableIdSet.isEmpty());
    }

    @Test
    public void testGetSynapseSurveyTablesNoItem() {
        // mock Synapse survey table
        when(mockSynapseSurveyTable.getItem("studyId", "test-app")).thenReturn(null);

        // execute and validate
        Set<String> tableIdSet = dynamoHelper.getSynapseSurveyTablesForApp("test-app");
        assertTrue(tableIdSet.isEmpty());
    }

    @Test
    public void testGetSynapseTablesAndSchemas() throws Exception {
        // There are 3 sub-cases to test here
        // * foo schema has no table
        // * bar schema has a table
        // * qwerty and asdf schemas both point to the same table

        // Mock Schema table App index. This involves stubbing out queryHelper() because indices can't be mocked
        // directly.
        List<Item> mockSchemaAppIndexResult = new ArrayList<>();
        mockSchemaAppIndexResult.add(makeUploadSchemaDdbItem("test-app", "foo", 1, null));
        mockSchemaAppIndexResult.add(makeUploadSchemaDdbItem("test-app", "bar", 2, null));
        mockSchemaAppIndexResult.add(makeUploadSchemaDdbItem("test-app", "qwerty", 3, null));
        mockSchemaAppIndexResult.add(makeUploadSchemaDdbItem("test-app", "asdf", 4, null));

        when(mockQueryHelper.query(mockSchemaAppIndex, "studyId", "test-app"))
                .thenReturn(mockSchemaAppIndexResult);

        // mock schema table
        when(mockSchemaTable.getItem("key", "test-app:foo", "revision", 1)).thenReturn(makeUploadSchemaDdbItem(
                "test-app", "foo", 1, DUMMY_FIELD_DEF_LIST_JSON));
        when(mockSchemaTable.getItem("key", "test-app:bar", "revision", 2)).thenReturn(makeUploadSchemaDdbItem(
                "test-app", "bar", 2, DUMMY_FIELD_DEF_LIST_JSON));
        when(mockSchemaTable.getItem("key", "test-app:qwerty", "revision", 3)).thenReturn(makeUploadSchemaDdbItem(
                "test-app", "qwerty", 3, DUMMY_FIELD_DEF_LIST_JSON));
        when(mockSchemaTable.getItem("key", "test-app:asdf", "revision", 4)).thenReturn(makeUploadSchemaDdbItem(
                "test-app", "asdf", 4, DUMMY_FIELD_DEF_LIST_JSON));

        // mock synapse map table
        when(mockSynapseMapTable.getItem("schemaKey", "test-app-bar-v2")).thenReturn(makeSynapseMapDdbItem(
                "test-app-bar-v2", "bar-table-id"));
        when(mockSynapseMapTable.getItem("schemaKey", "test-app-qwerty-v3")).thenReturn(makeSynapseMapDdbItem(
                "test-app-qwerty-v3", "qwerty-asdf-table-id"));
        when(mockSynapseMapTable.getItem("schemaKey", "test-app-asdf-v4")).thenReturn(makeSynapseMapDdbItem(
                "test-app-asdf-v4", "qwerty-asdf-table-id"));

        // execute and validate - Just check the key equals the schema we expect. Deep validation of schemas is done
        // in the schema tests
        Map<String, UploadSchema> synapseToSchemaMap = dynamoHelper.getSynapseTableIdsForApp("test-app");
        assertEquals(synapseToSchemaMap.size(), 2);
        assertEquals(synapseToSchemaMap.get("bar-table-id").getKey().toString(), "test-app-bar-v2");
        assertEquals(synapseToSchemaMap.get("qwerty-asdf-table-id").getKey().toString(), "test-app-asdf-v4");
    }

    private static Item makeUploadSchemaDdbItem(String appId, String schemaId, int rev, String fieldDefListJson) {
        Item retval = new Item().withString("studyId", appId).withString("key", appId + ":" + schemaId)
                .withInt("revision", rev);
        if (fieldDefListJson != null) {
            retval.withString("fieldDefinitions", fieldDefListJson);
        }
        return retval;
    }

    private static Item makeSynapseMapDdbItem(String schemaKey, String synapseTableId) {
        return new Item().withString("schemaKey", schemaKey)
                .withString(DynamoHelper.ATTR_TABLE_ID, synapseTableId);
    }

    @Test
    public void deleteSynapseTableIdMapping() {
        // Execute and validate.
        dynamoHelper.deleteSynapseTableIdMapping(TEST_SCHEMA_KEY);
        verify(mockSynapseMapTable).deleteItem("schemaKey", TEST_SCHEMA_KEY.toString());
    }

    @Test
    public void writeWorkerLog() {
        // Execute
        dynamoHelper.writeWorkerLog("dummy worker", "dummy tag");

        // Validate back-end
        ArgumentCaptor<Item> itemCaptor = ArgumentCaptor.forClass(Item.class);
        verify(mockWorkerLogTable).putItem(itemCaptor.capture());

        Item item = itemCaptor.getValue();
        assertEquals(item.getString(DynamoHelper.KEY_WORKER_ID), "dummy worker");
        assertEquals(item.getLong(DynamoHelper.KEY_FINISH_TIME), MOCK_NOW_MILLIS);
        assertEquals(item.getString(DynamoHelper.KEY_TAG), "dummy tag");
    }
}
