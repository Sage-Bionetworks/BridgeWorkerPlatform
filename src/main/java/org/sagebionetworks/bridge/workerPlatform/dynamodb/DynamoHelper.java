package org.sagebionetworks.bridge.workerPlatform.dynamodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.jcabi.aspects.Cacheable;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.notification.worker.NotificationType;
import org.sagebionetworks.bridge.notification.worker.UserNotification;
import org.sagebionetworks.bridge.notification.worker.WorkerConfig;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

/** Abstracts away calls to DynamoDB. */
@Component("DynamoHelper")
public class DynamoHelper {
    // DDB column names. Package-scoped for unit tests.
    static final String ATTR_APP_ID = "studyId";
    static final String ATTR_TABLE_ID = "tableId";
    static final String ATTR_TABLE_ID_SET = "tableIdSet";
    static final String ATTR_TABLE_NAME = "tableName";
    static final String KEY_APP_URL = "appUrl";
    static final String KEY_BURST_DURATION_DAYS = "burstDurationDays";
    static final String KEY_BURST_EVENT_ID_SET = "burstStartEventIdSet";
    static final String KEY_BURST_TASK_ID = "burstTaskId";
    static final String KEY_DEFAULT_PREBURST_MESSAGE = "defaultPreburstMessage";
    static final String KEY_EARLY_LATE_CUTOFF_DAYS = "earlyLateCutoffDays";
    static final String KEY_ENGAGEMENT_SURVEY_GUID = "engagementSurveyGuid";
    static final String KEY_EXCLUDED_DATA_GROUP_SET = "excludedDataGroupSet";
    static final String KEY_FINISH_TIME = "finishTime";
    static final String KEY_MESSAGE = "message";
    static final String KEY_MISSED_CUMULATIVE_MESSAGES = "missedCumulativeActivitiesMessagesList";
    static final String KEY_MISSED_EARLY_MESSAGES = "missedEarlyActivitiesMessagesList";
    static final String KEY_MISSED_LATER_MESSAGES = "missedLaterActivitiesMessagesList";
    static final String KEY_NOTIFICATION_BLACKOUT_DAYS_FROM_START = "notificationBlackoutDaysFromStart";
    static final String KEY_NOTIFICATION_BLACKOUT_DAYS_FROM_END = "notificationBlackoutDaysFromEnd";
    static final String KEY_NOTIFICATION_TIME = "notificationTime";
    static final String KEY_NOTIFICATION_TYPE = "notificationType";
    static final String KEY_NUM_ACTIVITIES_TO_COMPLETE = "numActivitiesToCompleteBurst";
    static final String KEY_NUM_MISSED_DAYS_TO_NOTIFY = "numMissedDaysToNotify";
    static final String KEY_NUM_MISSED_CONSECUTIVE_DAYS_TO_NOTIFY = "numMissedConsecutiveDaysToNotify";
    static final String KEY_PREBURST_MESSAGES = "preburstMessagesByDataGroup";
    static final String KEY_APP_ID = "studyId";
    static final String KEY_TAG = "tag";
    static final String KEY_USER_ID = "userId";
    static final String KEY_WORKER_ID = "workerId";
    static final String SUFFIX_DEFAULT = "-default";

    private Table ddbNotificationConfigTable;
    private Table ddbNotificationLogTable;
    private Table ddbAppTable;
    private Table ddbSynapseMapTable;
    private Table ddbSynapseMetaTable;
    private Table ddbSynapseSurveyTablesTable;
    private Table ddbUploadSchemaTable;
    private Index ddbUploadSchemaAppIndex;
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

    /** App table. */
    @Resource(name = "ddbAppTable")
    public final void setDdbAppTable(Table ddbAppTable) {
        this.ddbAppTable = ddbAppTable;
    }

    /** DDB table that maps upload schemas to Synapse table IDs. */
    @Resource(name = "ddbSynapseMapTable")
    public final void setDdbSynapseMapTable(Table ddbSynapseMapTable) {
        this.ddbSynapseMapTable = ddbSynapseMapTable;
    }

    /** DDB table with Synapse meta tables (appVersion, default (schemaless)). */
    @Resource(name = "ddbSynapseMetaTable")
    public final void setDdbSynapseMetaTable(Table ddbSynapseMetaTable) {
        this.ddbSynapseMetaTable = ddbSynapseMetaTable;
    }

    /**
     * DDB table that gets the list of all survey tables for a given app. Naming note: This is a DDB table containing
     * references to a set of Synapse tables. The name is a bit confusing,  but I'm not sure how to make it less
     * confusing.
     */
    @Resource(name = "ddbSynapseSurveyTablesTable")
    public final void setDdbSynapseSurveyTablesTable(Table ddbSynapseSurveyTablesTable) {
        this.ddbSynapseSurveyTablesTable = ddbSynapseSurveyTablesTable;
    }

    /** Upload schema table. */
    @Resource(name = "ddbUploadSchemaTable")
    public final void setDdbUploadSchemaTable(Table ddbUploadSchemaTable) {
        this.ddbUploadSchemaTable = ddbUploadSchemaTable;
    }

    /** UploadSchema studyId-index. For legacy reasons the App table was named Study 
     * and so it is referenced as studyId in many tables. Equivalent to appId. */
    @Resource(name = "ddbUploadSchemaAppIndex")
    public final void setDdbUploadSchemaAppIndex(Index ddbUploadSchemaAppIndex) {
        this.ddbUploadSchemaAppIndex = ddbUploadSchemaAppIndex;
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

    /**
     * Gets the ID for the default (schemaless) record table for the app. If the table doesn't exist, returns null.
     */
    public String getDefaultSynapseTableForApp(String appId) {
        Item item = ddbSynapseMetaTable.getItem(ATTR_TABLE_NAME, appId + SUFFIX_DEFAULT);
        if (item == null) {
            // Schemaless table hasn't been created yet. Skip.
            return null;
        }
        return item.getString(ATTR_TABLE_ID);
    }

    /**
     * Deletes the ID for the default (schemaless) record table from the meta table. This is generally used for when
     * the table is already deleted and we want to clean up.
     */
    public void deleteDefaultSynapseTableForApp(String appId) {
        ddbSynapseMetaTable.deleteItem(ATTR_TABLE_NAME, appId + SUFFIX_DEFAULT);
    }

    /** Gets the notification config for the given app. This method caches results for 5 minutes. */
    @SuppressWarnings("DefaultAnnotationParam")
    @Cacheable(lifetime = 5, unit = TimeUnit.MINUTES)
    public WorkerConfig getNotificationConfigForApp(String appId) {
        Item item = ddbNotificationConfigTable.getItem(KEY_APP_ID, appId);
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setAppUrl(item.getString(KEY_APP_URL));
        workerConfig.setBurstDurationDays(item.getInt(KEY_BURST_DURATION_DAYS));
        workerConfig.setBurstStartEventIdSet(item.getStringSet(KEY_BURST_EVENT_ID_SET));
        workerConfig.setBurstTaskId(item.getString(KEY_BURST_TASK_ID));
        workerConfig.setDefaultPreburstMessage(item.getString(KEY_DEFAULT_PREBURST_MESSAGE));
        workerConfig.setEarlyLateCutoffDays(item.getInt(KEY_EARLY_LATE_CUTOFF_DAYS));
        workerConfig.setEngagementSurveyGuid(item.getString(KEY_ENGAGEMENT_SURVEY_GUID));
        workerConfig.setExcludedDataGroupSet(item.getStringSet(KEY_EXCLUDED_DATA_GROUP_SET));
        workerConfig.setMissedCumulativeActivitiesMessagesList(item.getList(KEY_MISSED_CUMULATIVE_MESSAGES));
        workerConfig.setMissedEarlyActivitiesMessagesList(item.getList(KEY_MISSED_EARLY_MESSAGES));
        workerConfig.setMissedLaterActivitiesMessagesList(item.getList(KEY_MISSED_LATER_MESSAGES));
        workerConfig.setNotificationBlackoutDaysFromStart(item.getInt(KEY_NOTIFICATION_BLACKOUT_DAYS_FROM_START));
        workerConfig.setNotificationBlackoutDaysFromEnd(item.getInt(KEY_NOTIFICATION_BLACKOUT_DAYS_FROM_END));
        workerConfig.setNumActivitiesToCompleteBurst(item.getInt(KEY_NUM_ACTIVITIES_TO_COMPLETE));
        workerConfig.setNumMissedConsecutiveDaysToNotify(item.getInt(KEY_NUM_MISSED_CONSECUTIVE_DAYS_TO_NOTIFY));
        workerConfig.setNumMissedDaysToNotify(item.getInt(KEY_NUM_MISSED_DAYS_TO_NOTIFY));
        workerConfig.setPreburstMessagesByDataGroup(item.getMap(KEY_PREBURST_MESSAGES));
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

    /**
     * Gets app info for the given app ID.
     *
     * @param appId
     *         ID of app to fetch
     * @return the requested app
     */
    public AppInfo getApp(String appId) {
        Item app = ddbAppTable.getItem("identifier", appId);

        String appName = app.getString("name");
        String appShortName = app.getString("shortName");
        String supportEmail = app.getString("supportEmail");

        return new AppInfo.Builder().withName(appName).withShortName(appShortName).withAppId(appId)
                .withSupportEmail(supportEmail).build();
    }

    /**
     * Gets the set of survey table IDs for a given app.
     *
     * @param appId
     *         ID of app to get survey tables
     * @return set of survey table IDs, may be empty, but will never be null
     */
    public Set<String> getSynapseSurveyTablesForApp(String appId) {
        Item item = ddbSynapseSurveyTablesTable.getItem(ATTR_APP_ID, appId);
        if (item == null) {
            return ImmutableSet.of();
        }

        Set<String> tableIdSet = item.getStringSet(ATTR_TABLE_ID_SET);
        if (tableIdSet == null) {
            return ImmutableSet.of();
        }

        return tableIdSet;
    }

    /**
     * <p>
     * Deletes the survey from the survey table mapping. This is generally used for when the Synapse table is already
     * deleted and we want to clean up.
     * </p>
     * <p>
     * Synchronized, because different survey tasks are executed in parallel, so we want to avoid race conditions.
     * </p>
     */
    public synchronized void deleteSynapseSurveyTableMapping(String appId, String tableId) {
        Item item = ddbSynapseSurveyTablesTable.getItem(ATTR_APP_ID, appId);
        if (item == null) {
            // Somehow, the app doesn't exist in the survey table mapping anymore. Nothing to delete.
            return;
        }

        Set<String> tableIdSet = item.getStringSet(ATTR_TABLE_ID_SET);
        if (tableIdSet == null || !tableIdSet.contains(tableId)) {
            // Somehow, the app doesn't contain the specified table ID anymore. Nothing to delete.
            return;
        }

        // Update the mapping and save it back to DDB.
        tableIdSet.remove(tableId);
        if (tableIdSet.isEmpty()) {
            // DDB doesn't like empty sets. Null out the set.
            tableIdSet = null;
        }
        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey(ATTR_APP_ID, appId)
                .withUpdateExpression("set " + ATTR_TABLE_ID_SET + "=:s")
                .withValueMap(new ValueMap().withStringSet(":s", tableIdSet));
        ddbSynapseSurveyTablesTable.updateItem(updateItemSpec);
    }

    /**
     * Gets the Synapse table IDs associated with this app. The results are returned as a map from the Synapse table
     * IDs to the Bridge upload schemas.
     *
     * @param appId
     *         ID of the app to query on
     * @return map from the Synapse table IDs to the Bridge upload schema keys, may be empty, but will never be null
     */
    public Map<String, UploadSchema> getSynapseTableIdsForApp(String appId) throws IOException {
        // query and iterate
        List<UploadSchema> schemaList = new ArrayList<>();
        Iterable<Item> schemaItemIter = dynamoQueryHelper.query(ddbUploadSchemaAppIndex, ATTR_APP_ID, appId);
        for (Item oneSchemaItem : schemaItemIter) {
            // Index only contains app ID, key, and revision. Re-query the table to get all fields.
            String key = oneSchemaItem.getString("key");
            int rev = oneSchemaItem.getInt("revision");
            Item fullSchemaItem = ddbUploadSchemaTable.getItem("key", key, "revision", rev);

            UploadSchema schema = UploadSchema.fromDdbItem(fullSchemaItem);
            schemaList.add(schema);
        }

        // Now query the SynapseTables table to get the Synapse table IDs for the schema. We use a reverse map from
        // Synapse table ID to upload schema, because multiple upload schemas can map to a single Synapse table. (This
        // is due to some early day hacks in the original apps.)
        Multimap<String, UploadSchema> synapseToSchemaMultimap = HashMultimap.create();
        for (UploadSchema oneSchema : schemaList) {
            Item synapseMapRecord = ddbSynapseMapTable.getItem("schemaKey", oneSchema.getKey().toString());
            if (synapseMapRecord == null) {
                // This could happen if the schema was just created, but the Bridge-Exporter hasn't created the
                // corresponding Synapse table yet. If so, there's obviously no data. Skip this one.
                continue;
            }

            String synapseTableId = synapseMapRecord.getString(ATTR_TABLE_ID);
            synapseToSchemaMultimap.put(synapseTableId, oneSchema);
        }

        // Dedupe the upload schemas. We pick the canonical schema based on which one has the highest rev.
        Map<String, UploadSchema> synapseToSchemaMap = new HashMap<>();
        for (String oneSynapseTableId : synapseToSchemaMultimap.keySet()) {
            Iterable<UploadSchema> schemaIter = synapseToSchemaMultimap.get(oneSynapseTableId);
            UploadSchema canonicalSchema = null;
            for (UploadSchema oneSchema : schemaIter) {
                if (canonicalSchema == null ||
                        canonicalSchema.getKey().getRevision() < oneSchema.getKey().getRevision()) {
                    canonicalSchema = oneSchema;
                }
            }

            // Because of the way this code is written, there will always be at least one schema for this table ID, so
            // by this point, canonicalSchema won't be null.
            synapseToSchemaMap.put(oneSynapseTableId, canonicalSchema);
        }

        return synapseToSchemaMap;
    }

    /**
     * Deletes the schema key from the schema to table mapping. This is generally used for when the Synapse table is
     * already deleted and we want to clean up.
     */
    public void deleteSynapseTableIdMapping(UploadSchemaKey schemaKey) {
        ddbSynapseMapTable.deleteItem("schemaKey", schemaKey.toString());
    }

    /** Writes the worker run to the worker log, with the current timestamp and the given tag. */
    public void writeWorkerLog(String workerId, String tag) {
        Item item = new Item().withPrimaryKey(KEY_WORKER_ID, workerId, KEY_FINISH_TIME,
                DateTime.now().getMillis()).withString(KEY_TAG, tag);
        ddbWorkerLogTable.putItem(item);
    }
}
