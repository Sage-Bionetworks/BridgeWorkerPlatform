package org.sagebionetworks.bridge.workerPlatform.helper;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DynamoHelperTest {
    private static final long MOCK_NOW_MILLIS = DateTime.parse("2018-04-27T16:41:15.831-0700").getMillis();

    private DynamoHelper dynamoHelper;
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
        mockWorkerLogTable = mock(Table.class);

        // Create DynamoHelper
        dynamoHelper = new DynamoHelper();
        dynamoHelper.setDdbWorkerLogTable(mockWorkerLogTable);
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
