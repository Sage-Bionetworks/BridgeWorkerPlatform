package org.sagebionetworks.bridge.notification.helper;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.jcabi.aspects.Cacheable;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.notification.worker.NotificationType;
import org.sagebionetworks.bridge.notification.worker.UserNotification;
import org.sagebionetworks.bridge.notification.worker.WorkerConfig;

/** Abstracts away DynamoDB calls. */
@Component("NotificationWorkerDynamoHelper")
public class DynamoHelper {
    // DDB column names. Package-scoped for unit tests.
    static final String KEY_APP_URL = "appUrl";
    static final String KEY_BURST_DURATION_DAYS = "burstDurationDays";
    static final String KEY_BURST_EVENT_ID_SET = "burstStartEventIdSet";
    static final String KEY_BURST_TASK_ID = "burstTaskId";
    static final String KEY_EARLY_LATE_CUTOFF_DAYS = "earlyLateCutoffDays";
    static final String KEY_ENGAGEMENT_SURVEY_GUID = "engagementSurveyGuid";
    static final String KEY_EXCLUDED_DATA_GROUP_SET = "excludedDataGroupSet";
    static final String KEY_FINISH_TIME = "finishTime";
    static final String KEY_MESSAGE = "message";
    static final String KEY_MISSED_CUMULATIVE_MESSAGES = "missedCumulativeActivitiesMessagesByDataGroup";
    static final String KEY_MISSED_EARLY_MESSAGES = "missedEarlyActivitiesMessagesByDataGroup";
    static final String KEY_MISSED_LATER_MESSAGES = "missedLaterActivitiesMessagesByDataGroup";
    static final String KEY_NOTIFICATION_BLACKOUT_DAYS_FROM_START = "notificationBlackoutDaysFromStart";
    static final String KEY_NOTIFICATION_BLACKOUT_DAYS_FROM_END = "notificationBlackoutDaysFromEnd";
    static final String KEY_NOTIFICATION_TIME = "notificationTime";
    static final String KEY_NOTIFICATION_TYPE = "notificationType";
    static final String KEY_NUM_ACTIVITIES_TO_COMPLETE = "numActivitiesToCompleteBurst";
    static final String KEY_NUM_MISSED_DAYS_TO_NOTIFY = "numMissedDaysToNotify";
    static final String KEY_NUM_MISSED_CONSECUTIVE_DAYS_TO_NOTIFY = "numMissedConsecutiveDaysToNotify";
    static final String KEY_PREBURST_MESSAGES = "preburstMessagesByDataGroup";
    static final String KEY_REQUIRED_DATA_GROUPS = "requiredDataGroupsOneOfSet";
    static final String KEY_REQUIRED_SUBPOPULATION_GUID_SET = "requiredSubpopulationGuidSet";
    static final String KEY_STUDY_ID = "studyId";
    static final String KEY_TAG = "tag";
    static final String KEY_USER_ID = "userId";
    static final String KEY_WORKER_ID = "workerId";

    // Worker ID for the Worker Log
    static final String VALUE_WORKER_ID = "ActivityNotificationWorker";

    private Table ddbNotificationConfigTable;
    private Table ddbNotificationLogTable;
    private Table ddbWorkerLogTable;
    private DynamoQueryHelper dynamoQueryHelper;

    /** DDB table for notification configs. */
    @Resource(name = "ddbNotificationConfigTable")
    public final void setDdbNotificationConfigTable(Table ddbNotificationConfigTable) {
        this.ddbNotificationConfigTable = ddbNotificationConfigTable;
    }

    /** DDB table for notification logs, used to track which users have received notifications and when. */
    @Resource(name = "ddbNotificationLogTable")
    public final void setDdbNotificationLogTable(Table ddbNotificationLogTable) {
        this.ddbNotificationLogTable = ddbNotificationLogTable;
    }

    /**
     * DDB table for the worker log. Used to track worker runs and to signal to integration tests when the worker has
     * finished running.
     */
    @Resource(name = "ddbWorkerLogTable")
    public final void setDdbWorkerLogTable(Table ddbWorkerLogTable) {
        this.ddbWorkerLogTable = ddbWorkerLogTable;
    }

    /** DDB query helper, used to abstract away query logic and typing. */
    @Autowired
    public final void setDynamoQueryHelper(DynamoQueryHelper dynamoQueryHelper) {
        this.dynamoQueryHelper = dynamoQueryHelper;
    }

    /** Gets the notification config for the given study. This method caches results for 5 minutes. */
    @Cacheable(lifetime = 5, unit = TimeUnit.MINUTES)
    public WorkerConfig getNotificationConfigForStudy(String studyId) {
        Item item = ddbNotificationConfigTable.getItem(KEY_STUDY_ID, studyId);
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setAppUrl(item.getString(KEY_APP_URL));
        workerConfig.setBurstDurationDays(item.getInt(KEY_BURST_DURATION_DAYS));
        workerConfig.setBurstStartEventIdSet(item.getStringSet(KEY_BURST_EVENT_ID_SET));
        workerConfig.setBurstTaskId(item.getString(KEY_BURST_TASK_ID));
        workerConfig.setEarlyLateCutoffDays(item.getInt(KEY_EARLY_LATE_CUTOFF_DAYS));
        workerConfig.setEngagementSurveyGuid(item.getString(KEY_ENGAGEMENT_SURVEY_GUID));
        workerConfig.setExcludedDataGroupSet(item.getStringSet(KEY_EXCLUDED_DATA_GROUP_SET));
        workerConfig.setMissedCumulativeActivitiesMessagesByDataGroup(item.getMap(KEY_MISSED_CUMULATIVE_MESSAGES));
        workerConfig.setMissedEarlyActivitiesMessagesByDataGroup(item.getMap(KEY_MISSED_EARLY_MESSAGES));
        workerConfig.setMissedLaterActivitiesMessagesByDataGroup(item.getMap(KEY_MISSED_LATER_MESSAGES));
        workerConfig.setNotificationBlackoutDaysFromStart(item.getInt(KEY_NOTIFICATION_BLACKOUT_DAYS_FROM_START));
        workerConfig.setNotificationBlackoutDaysFromEnd(item.getInt(KEY_NOTIFICATION_BLACKOUT_DAYS_FROM_END));
        workerConfig.setNumActivitiesToCompleteBurst(item.getInt(KEY_NUM_ACTIVITIES_TO_COMPLETE));
        workerConfig.setNumMissedConsecutiveDaysToNotify(item.getInt(KEY_NUM_MISSED_CONSECUTIVE_DAYS_TO_NOTIFY));
        workerConfig.setNumMissedDaysToNotify(item.getInt(KEY_NUM_MISSED_DAYS_TO_NOTIFY));
        workerConfig.setPreburstMessagesByDataGroup(item.getMap(KEY_PREBURST_MESSAGES));
        workerConfig.setRequiredDataGroupsOneOfSet(item.getStringSet(KEY_REQUIRED_DATA_GROUPS));
        workerConfig.setRequiredSubpopulationGuidSet(item.getStringSet(KEY_REQUIRED_SUBPOPULATION_GUID_SET));
        return workerConfig;
    }

    /**
     * Gets the notification info for the given user's most recent notification. Returns null if the user has
     * never been sent a notification.
     */
    public UserNotification getLastNotificationTimeForUser(String userId) {
        // To get the latest notification time, sort the index in reverse and limit the result set to 1.
        QuerySpec query = new QuerySpec().withHashKey(KEY_USER_ID, userId).withScanIndexForward(false)
                .withMaxResultSize(1);
        Iterator<Item> itemIter = dynamoQueryHelper.query(ddbNotificationLogTable, query).iterator();
        if (itemIter.hasNext()) {
            Item item = itemIter.next();

            UserNotification userNotification = new UserNotification();
            userNotification.setMessage(item.getString(KEY_MESSAGE));
            userNotification.setTime(item.getLong(KEY_NOTIFICATION_TIME));
            userNotification.setUserId(item.getString(KEY_USER_ID));

            // Parse notification type. Need a null check in case of old notification logs that pre-date this enum.
            String notificationTypeString = item.getString(KEY_NOTIFICATION_TYPE);
            if (StringUtils.isNotBlank(notificationTypeString)) {
                userNotification.setType(NotificationType.valueOf(notificationTypeString));
            } else {
                userNotification.setType(NotificationType.UNKNOWN);
            }

            return userNotification;
        } else {
            return null;
        }
    }

    /** Appends the notification info to the notification log for the given user. */
    public void setLastNotificationTimeForUser(UserNotification userNotification) {
        Item item = new Item().withPrimaryKey(KEY_USER_ID, userNotification.getUserId(),
                KEY_NOTIFICATION_TIME, userNotification.getTime())
                .withString(KEY_MESSAGE, userNotification.getMessage())
                .withString(KEY_NOTIFICATION_TYPE, userNotification.getType().name());
        ddbNotificationLogTable.putItem(item);
    }

    /** Writes the Notification Worker to the worker log, with the current timestamp and the given tag. */
    public void writeWorkerLog(String tag) {
        Item item = new Item().withPrimaryKey(KEY_WORKER_ID, VALUE_WORKER_ID, KEY_FINISH_TIME,
                DateTime.now().getMillis()).withString(KEY_TAG, tag);
        ddbWorkerLogTable.putItem(item);
    }
}
