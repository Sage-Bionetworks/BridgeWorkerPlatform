package org.sagebionetworks.bridge.workerPlatform.multiplexer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.reporter.worker.BridgeReporterProcessor;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.udd.worker.BridgeUddProcessor;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.util.Constants;

public class BridgeWorkerPlatformSqsCallbackTest {
    // simple strings for test
    private static final String TEST_SCHEDULER = "test-scheduler";
    private static final String TEST_STUDY_ID = "api";
    private static final String TEST_EMAIL = "zhizhen.lin@sagebase.org";
    private static final ReportType TEST_SCHEDULE_TYPE = ReportType.DAILY;

    private static final String REQUEST_JSON_MSG = "{\n" +
            "   \"service\":\"REPORTER\",\n" +
            "   \"body\":" + "{\n" +
            "       \"scheduler\":\"test-scheduler\",\n" +
            "       \"scheduleType\":\"DAILY\",\n" +
            "       \"startDateTime\":\"2016-10-19T00:00:00.000Z\",\n" +
            "       \"endDateTime\":\"2016-10-20T23:59:59.000Z\"\n" +
            "   }\n" +
            "}";

    private static final String REQUEST_JSON_UDD_MSG = "{\n" +
            "   \"service\":\"UDD\",\n" +
            "   \"body\":" + "{\n" +
            "       \"studyId\":\"" + TEST_STUDY_ID +"\",\n" +
            "       \"username\":\"" + TEST_EMAIL + "\",\n" +
            "       \"startDate\":\"2015-03-09\",\n" +
            "       \"endDate\":\"2015-03-31\"\n" +
            "   }\n" +
            "}";

    private static final String REPORTER_REQUEST = "{\n" +
            "   \"scheduler\":\"" + TEST_SCHEDULER +"\",\n" +
            "   \"scheduleType\":\"" + TEST_SCHEDULE_TYPE.toString() + "\",\n" +
            "   \"startDateTime\":\"2016-10-19T00:00:00.000Z\",\n" +
            "   \"endDateTime\":\"2016-10-20T23:59:59.000Z\"\n" +
            "}";

    private static final String UDD_REQUEST = "{\n" +
            "   \"studyId\":\"" + TEST_STUDY_ID +"\",\n" +
            "   \"username\":\"" + TEST_EMAIL + "\",\n" +
            "   \"startDate\":\"2015-03-09\",\n" +
            "   \"endDate\":\"2015-03-31\"\n" +
            "}";

    // This exception is used to test error propagation.
    @SuppressWarnings("serial")
    private static class TestException extends RuntimeException {
    }

    private JsonNode reporterRequestJson;
    private JsonNode uddRequestJson;

    // test members
    private BridgeWorkerPlatformSqsCallback callback;
    private BridgeReporterProcessor mockReporterProcessor;
    private BridgeUddProcessor mockUddProcessor;

    @BeforeClass
    public void generalSetup() throws IOException {
        reporterRequestJson = DefaultObjectMapper.INSTANCE.readValue(REPORTER_REQUEST, JsonNode.class);
        uddRequestJson = DefaultObjectMapper.INSTANCE.readValue(UDD_REQUEST, JsonNode.class);
    }

    @BeforeMethod
    public void setup() throws Exception {
        mockReporterProcessor = mock(BridgeReporterProcessor.class);
        mockUddProcessor = mock(BridgeUddProcessor.class);

        // set up callback
        callback = new BridgeWorkerPlatformSqsCallback();
        callback.setWorkersByServiceName(ImmutableMap.<String, ThrowingConsumer<JsonNode>>builder()
                .put(Constants.SERVICE_TYPE_REPORTER, mockReporterProcessor::process)
                .put(Constants.SERVICE_TYPE_UDD, mockUddProcessor::process)
                .build());
    }

    @Test
    public void testBridgeReporter() throws Exception {
        callback.callback(REQUEST_JSON_MSG);
        verify(mockReporterProcessor).process(eq(reporterRequestJson));
    }

    @Test(expectedExceptions = TestException.class)
    public void testBridgeReporterException() throws Exception {
        doThrow(TestException.class).when(mockReporterProcessor).process(any());
        callback.callback(REQUEST_JSON_MSG);
    }

    @Test
    public void testBridgeUdd() throws Exception {
        callback.callback(REQUEST_JSON_UDD_MSG);
        verify(mockUddProcessor).process(eq(uddRequestJson));
    }

    @Test(expectedExceptions = TestException.class)
    public void testBridgeUddException() throws Exception {
        doThrow(TestException.class).when(mockUddProcessor).process(any());
        callback.callback(REQUEST_JSON_UDD_MSG);
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void malformedRequest() throws Exception {
        callback.callback("not json");
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class, expectedExceptionsMessageRegExp =
            "Invalid service not-a-service")
    public void invalidService() throws Exception {
        String requestText = "{\n" +
                "   \"service\":\"not-a-service\",\n" +
                "   \"body\":{\"foo\":\"bar\"}\n" +
                "}";
        callback.callback(requestText);
    }
}
