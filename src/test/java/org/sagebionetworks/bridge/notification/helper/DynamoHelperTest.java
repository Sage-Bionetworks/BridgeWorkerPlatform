package org.sagebionetworks.bridge.notification.helper;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.notification.worker.NotificationType;
import org.sagebionetworks.bridge.notification.worker.UserNotification;
import org.sagebionetworks.bridge.notification.worker.WorkerConfig;

public class DynamoHelperTest {
    private static final long MOCK_NOW_MILLIS = DateTime.parse("2018-04-27T16:41:15.831-0700").getMillis();
    private static final String STUDY_ID = "test-study";
    private static final String USER_ID = "test-user";

    private DynamoHelper dynamoHelper;
    private DynamoQueryHelper mockQueryHelper;
    private Table mockNotificationConfigTable;
    private Table mockNotificationLogTable;
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
        mockQueryHelper = mock(DynamoQueryHelper.class);
        mockNotificationConfigTable = mock(Table.class);
        mockNotificationLogTable = mock(Table.class);
        mockWorkerLogTable = mock(Table.class);

        // Create DynamoHelper
        dynamoHelper = new DynamoHelper();
        dynamoHelper.setDynamoQueryHelper(mockQueryHelper);
        dynamoHelper.setDdbNotificationConfigTable(mockNotificationConfigTable);
        dynamoHelper.setDdbNotificationLogTable(mockNotificationLogTable);
        dynamoHelper.setDdbWorkerLogTable(mockWorkerLogTable);
    }

    @Test
    public void getNotificationConfigForStudy() {
        // Set up dummy maps
        Map<String, String> missedCumulativeMessagesMap = ImmutableMap.of(
                "required-group-1", "cumulative-message-1",
                "required-group-2", "cumulative-message-2");
        Map<String, String> missedEarlyMessagesMap = ImmutableMap.of(
                "required-group-1", "early-message-1",
                "required-group-2", "early-message-2");
        Map<String, String> missedLaterMessagesMap = ImmutableMap.of(
                "required-group-1", "later-message-1",
                "required-group-2", "later-message-2");
        Map<String, String> preburstMessagesMap = ImmutableMap.of(
                "required-group-1", "preburst-message-1",
                "required-group-2", "preburst-message-2");

        // Set up mock
        Item item = new Item()
                .withPrimaryKey(DynamoHelper.KEY_STUDY_ID, STUDY_ID)
                .withInt(DynamoHelper.KEY_BURST_DURATION_DAYS, 19)
                .withStringSet(DynamoHelper.KEY_BURST_EVENT_ID_SET, "enrollment", "custom:activityBurst2Start")
                .withString(DynamoHelper.KEY_BURST_TASK_ID, "study-burst-task")
                .withInt(DynamoHelper.KEY_EARLY_LATE_CUTOFF_DAYS, 7)
                .withStringSet(DynamoHelper.KEY_EXCLUDED_DATA_GROUP_SET, "excluded-group-1", "excluded-group-2")
                .withMap(DynamoHelper.KEY_MISSED_CUMULATIVE_MESSAGES, missedCumulativeMessagesMap)
                .withMap(DynamoHelper.KEY_MISSED_EARLY_MESSAGES, missedEarlyMessagesMap)
                .withMap(DynamoHelper.KEY_MISSED_LATER_MESSAGES, missedLaterMessagesMap)
                .withInt(DynamoHelper.KEY_NOTIFICATION_BLACKOUT_DAYS_FROM_START, 2)
                .withInt(DynamoHelper.KEY_NOTIFICATION_BLACKOUT_DAYS_FROM_END, 1)
                .withInt(DynamoHelper.KEY_NUM_ACTIVITIES_TO_COMPLETE, 6)
                .withInt(DynamoHelper.KEY_NUM_MISSED_CONSECUTIVE_DAYS_TO_NOTIFY, 3)
                .withInt(DynamoHelper.KEY_NUM_MISSED_DAYS_TO_NOTIFY, 4)
                .withMap(DynamoHelper.KEY_PREBURST_MESSAGES, preburstMessagesMap)
                .withStringSet(DynamoHelper.KEY_REQUIRED_DATA_GROUPS, "required-group-1", "required-group-2")
                .withStringSet(DynamoHelper.KEY_REQUIRED_SUBPOPULATION_GUID_SET, STUDY_ID);
        when(mockNotificationConfigTable.getItem(DynamoHelper.KEY_STUDY_ID, STUDY_ID)).thenReturn(item);

        // Execute and validate
        WorkerConfig config = dynamoHelper.getNotificationConfigForStudy(STUDY_ID);
        assertEquals(config.getBurstDurationDays(), 19);
        assertEquals(config.getBurstStartEventIdSet(), ImmutableSet.of("enrollment",
                "custom:activityBurst2Start"));
        assertEquals(config.getBurstTaskId(), "study-burst-task");
        assertEquals(config.getEarlyLateCutoffDays(), 7);
        assertEquals(config.getExcludedDataGroupSet(), ImmutableSet.of("excluded-group-1",
                "excluded-group-2"));
        assertEquals(config.getMissedCumulativeActivitiesMessagesByDataGroup(), missedCumulativeMessagesMap);
        assertEquals(config.getMissedEarlyActivitiesMessagesByDataGroup(), missedEarlyMessagesMap);
        assertEquals(config.getMissedLaterActivitiesMessagesByDataGroup(), missedLaterMessagesMap);
        assertEquals(config.getNotificationBlackoutDaysFromStart(), 2);
        assertEquals(config.getNotificationBlackoutDaysFromEnd(), 1);
        assertEquals(config.getNumActivitiesToCompleteBurst(), 6);
        assertEquals(config.getNumMissedConsecutiveDaysToNotify(), 3);
        assertEquals(config.getNumMissedDaysToNotify(), 4);
        assertEquals(config.getPreburstMessagesByDataGroup(), preburstMessagesMap);
        assertEquals(config.getRequiredDataGroupsOneOfSet(), ImmutableSet.of("required-group-1",
                "required-group-2"));
        assertEquals(config.getRequiredSubpopulationGuidSet(), ImmutableSet.of(STUDY_ID));

        verify(mockNotificationConfigTable).getItem(DynamoHelper.KEY_STUDY_ID, STUDY_ID);

        // Test caching
        WorkerConfig config2 = dynamoHelper.getNotificationConfigForStudy(STUDY_ID);
        assertNotNull(config2);
        verifyNoMoreInteractions(mockNotificationConfigTable);
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
    public void writeWorkerLog() {
        // Execute
        dynamoHelper.writeWorkerLog("dummy tag");

        // Validate back-end
        ArgumentCaptor<Item> itemCaptor = ArgumentCaptor.forClass(Item.class);
        verify(mockWorkerLogTable).putItem(itemCaptor.capture());

        Item item = itemCaptor.getValue();
        assertEquals(item.getString(DynamoHelper.KEY_WORKER_ID), DynamoHelper.VALUE_WORKER_ID);
        assertEquals(item.getLong(DynamoHelper.KEY_FINISH_TIME), MOCK_NOW_MILLIS);
        assertEquals(item.getString(DynamoHelper.KEY_TAG), "dummy tag");
    }
}
