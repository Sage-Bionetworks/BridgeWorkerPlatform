package org.sagebionetworks.bridge.workerPlatform.multiplexer;

import static org.sagebionetworks.bridge.json.DefaultObjectMapper.INSTANCE;
import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.workerPlatform.util.Constants;

public class BridgeWorkerPlatformRequestTest {
    private static final String TEST_JSON_NODE_STRING = "{\n" +
            "   \"scheduler\":\"test-scheduler\",\n" +
            "   \"scheduleType\":\"DAILY\",\n" +
            "   \"startDateTime\":\"2016-10-19T00:00:00.000Z\",\n" +
            "   \"endDateTime\":\"2016-10-20T23:59:59.000Z\"\n" +
            "}";

    private static JsonNode testBody;

    @BeforeClass
    public void setup() throws Exception{
        testBody = DefaultObjectMapper.INSTANCE.readTree(TEST_JSON_NODE_STRING);
    }

    @Test
    public void normalCase() {
        BridgeWorkerPlatformRequest request = new BridgeWorkerPlatformRequest.Builder()
                .withService(Constants.SERVICE_TYPE_REPORTER).withBody(testBody).build();
        assertEquals(request.getService(), Constants.SERVICE_TYPE_REPORTER);
        assertEquals(request.getBody(), testBody);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*service.*")
    public void nullServiceType() {
        new BridgeWorkerPlatformRequest.Builder().withBody(testBody).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*body.*")
    public void nullBody() {
        new BridgeWorkerPlatformRequest.Builder().withService(Constants.SERVICE_TYPE_REPORTER).build();
    }

    @Test
    public void jsonSerialization() throws Exception {
        // start with JSON
        String jsonText = "{\n" +
                "   \"service\":\"REPORTER\",\n" +
                "   \"body\":" + "{\n" +
                "       \"scheduler\":\"test-scheduler\",\n" +
                "       \"scheduleType\":\"DAILY\",\n" +
                "       \"startDateTime\":\"2016-10-19T00:00:00.000Z\",\n" +
                "       \"endDateTime\":\"2016-10-20T23:59:59.000Z\"\n" +
                "   }\n" +
                "}";

        // convert to POJO
        BridgeWorkerPlatformRequest request = INSTANCE.readValue(jsonText, BridgeWorkerPlatformRequest.class);
        assertEquals(request.getBody(), testBody);
        assertEquals(request.getService(), Constants.SERVICE_TYPE_REPORTER);
    }
}
