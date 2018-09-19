package org.sagebionetworks.bridge.workerPlatform.helper;

import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.joda.time.DateTime;
import org.springframework.stereotype.Component;

/** Abstracts away calls to DynamoDB. */
// TODO consolidate all the other BridgeHelpers into this one
@Component("DynamoHelper")
public class DynamoHelper {
    static final String KEY_FINISH_TIME = "finishTime";
    static final String KEY_TAG = "tag";
    static final String KEY_WORKER_ID = "workerId";

    private Table ddbWorkerLogTable;

    /**
     * DDB table for the worker log. Used to track worker runs and to signal to integration tests when the worker has
     * finished running.
     */
    @Resource(name = "ddbWorkerLogTable")
    public final void setDdbWorkerLogTable(Table ddbWorkerLogTable) {
        this.ddbWorkerLogTable = ddbWorkerLogTable;
    }

    /** Writes the worker run to the worker log, with the current timestamp and the given tag. */
    public void writeWorkerLog(String workerId, String tag) {
        Item item = new Item().withPrimaryKey(KEY_WORKER_ID, workerId, KEY_FINISH_TIME,
                DateTime.now().getMillis()).withString(KEY_TAG, tag);
        ddbWorkerLogTable.putItem(item);
    }
}
