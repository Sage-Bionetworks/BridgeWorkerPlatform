package org.sagebionetworks.bridge.reporter.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.reporter.request.ReportType;

import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@SuppressWarnings("unchecked")
public class BridgeReporterRequestTest {
    private static final String TEST_SCHEDULER = "test-scheduler";
    private static final ReportType TEST_SCHEDULE_TYPE = ReportType.DAILY;
    private static final List<String> TEST_APP_WHITELIST = ImmutableList.of("foo", "bar", "baz");
    private static final DateTime TEST_START_DATETIME = DateTime.parse("2016-10-19T00:00:00Z");
    private static final DateTime TEST_END_DATETIME = DateTime.parse("2016-10-20T23:59:59Z");

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*scheduler.*")
    public void nullScheduler() {
        new BridgeReporterRequest.Builder().withScheduleType(TEST_SCHEDULE_TYPE).withStartDateTime(TEST_START_DATETIME)
                .withEndDateTime(TEST_END_DATETIME).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*scheduler.*")
    public void emptyScheduler() {
        new BridgeReporterRequest.Builder().withScheduler("").withScheduleType(TEST_SCHEDULE_TYPE).withStartDateTime(TEST_START_DATETIME)
                .withEndDateTime(TEST_END_DATETIME).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*scheduleType.*")
    public void nullScheduleType() {
        new BridgeReporterRequest.Builder().withScheduler(TEST_SCHEDULER).withStartDateTime(TEST_START_DATETIME)
                .withEndDateTime(TEST_END_DATETIME).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*startDateTime.*")
    public void nullStartDateTime() {
        new BridgeReporterRequest.Builder().withScheduler(TEST_SCHEDULER).withScheduleType(TEST_SCHEDULE_TYPE)
                .withEndDateTime(TEST_END_DATETIME).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*endDate.*")
    public void nullEndDateTime() {
        new BridgeReporterRequest.Builder().withScheduler(TEST_SCHEDULER).withScheduleType(TEST_SCHEDULE_TYPE)
                .withStartDateTime(TEST_START_DATETIME).build();
    }

    @Test
    public void normalCase() {
        BridgeReporterRequest request = new BridgeReporterRequest.Builder().withScheduler(TEST_SCHEDULER)
                .withScheduleType(TEST_SCHEDULE_TYPE)
                .withStartDateTime(TEST_START_DATETIME)
                .withEndDateTime(TEST_END_DATETIME).build();
        assertEquals(request.getScheduler(), TEST_SCHEDULER);
        assertEquals(request.getScheduleType(), TEST_SCHEDULE_TYPE);
        assertEquals(request.getStartDateTime(), TEST_START_DATETIME);
        assertTrue(request.getAppWhitelist().isEmpty());
        assertEquals(request.getEndDateTime(), TEST_END_DATETIME);
    }

    @Test
    public void startDateSameAsEndDate() {
        BridgeReporterRequest request = new BridgeReporterRequest.Builder().withScheduler(TEST_SCHEDULER)
                .withScheduleType(TEST_SCHEDULE_TYPE)
                .withStartDateTime(TEST_START_DATETIME)
                .withEndDateTime(TEST_START_DATETIME).build();

        // Most params are tested above. Just test the ones specific to this test.
        assertEquals(request.getStartDateTime(), TEST_START_DATETIME);
        assertEquals(request.getEndDateTime(), TEST_START_DATETIME);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void startDateAfterEndDate() {
        new BridgeReporterRequest.Builder().withScheduler(TEST_SCHEDULER)
                .withScheduleType(TEST_SCHEDULE_TYPE)
                .withStartDateTime(TEST_END_DATETIME)
                .withEndDateTime(TEST_START_DATETIME).build();
    }

    @Test
    public void withAppWhitelist() {
        BridgeReporterRequest request = new BridgeReporterRequest.Builder().withScheduler(TEST_SCHEDULER)
                .withScheduleType(TEST_SCHEDULE_TYPE)
                .withAppWhitelist(TEST_APP_WHITELIST)
                .withStartDateTime(TEST_START_DATETIME)
                .withEndDateTime(TEST_END_DATETIME).build();

        // Most params are tested above. Just test the ones specific to this test.
        assertEquals(request.getAppWhitelist(), TEST_APP_WHITELIST);
    }

    @Test
    public void jsonSerializationWithStudyWhitelist() throws Exception {
        // start with JSON
        String jsonText = "{\n" +
                "   \"scheduler\":\"test-scheduler\",\n" +
                "   \"scheduleType\":\"DAILY\",\n" +
                "   \"studyWhitelist\":[\"test-app\"],\n" +
                "   \"startDateTime\":\"2016-10-19T00:00:00.000Z\",\n" +
                "   \"endDateTime\":\"2016-10-20T23:59:59.000Z\"\n" +
                "}";

        // convert to POJO
        BridgeReporterRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText, BridgeReporterRequest.class);
        assertEquals(request.getScheduler(), TEST_SCHEDULER);
        assertEquals(request.getScheduleType(), TEST_SCHEDULE_TYPE);
        assertEquals(request.getStartDateTime(), TEST_START_DATETIME);
        assertEquals(request.getEndDateTime(), TEST_END_DATETIME);

        List<String> appWhitelist = request.getAppWhitelist();
        assertEquals(appWhitelist.size(), 1);
        assertEquals(appWhitelist.get(0), "test-app");

        // then convert to a map so we can validate the raw JSON
        JsonNode jsonNode = DefaultObjectMapper.INSTANCE.convertValue(request, JsonNode.class);
        assertEquals(jsonNode.size(), 5);
        assertEquals(jsonNode.get("scheduler").textValue(), TEST_SCHEDULER);
        assertEquals(jsonNode.get("scheduleType").textValue(), TEST_SCHEDULE_TYPE.getName());
        assertEquals(jsonNode.get("startDateTime").textValue(), TEST_START_DATETIME.toString());
        assertEquals(jsonNode.get("endDateTime").textValue(), TEST_END_DATETIME.toString());

        JsonNode appWhitelistNode = jsonNode.get("appWhitelist");
        assertEquals(appWhitelistNode.size(), 1);
        assertEquals(appWhitelistNode.get(0).textValue(), "test-app");
    }
    
    @Test
    public void jsonSerializationWithAppWhitelist() throws Exception {
        // start with JSON
        String jsonText = "{\n" +
                "   \"scheduler\":\"test-scheduler\",\n" +
                "   \"scheduleType\":\"DAILY\",\n" +
                "   \"appWhitelist\":[\"test-app\"],\n" +
                "   \"startDateTime\":\"2016-10-19T00:00:00.000Z\",\n" +
                "   \"endDateTime\":\"2016-10-20T23:59:59.000Z\"\n" +
                "}";

        // convert to POJO
        BridgeReporterRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText, BridgeReporterRequest.class);
        assertEquals(request.getScheduler(), TEST_SCHEDULER);
        assertEquals(request.getScheduleType(), TEST_SCHEDULE_TYPE);
        assertEquals(request.getStartDateTime(), TEST_START_DATETIME);
        assertEquals(request.getEndDateTime(), TEST_END_DATETIME);

        List<String> appWhitelist = request.getAppWhitelist();
        assertEquals(appWhitelist.size(), 1);
        assertEquals(appWhitelist.get(0), "test-app");

        // then convert to a map so we can validate the raw JSON
        JsonNode jsonNode = DefaultObjectMapper.INSTANCE.convertValue(request, JsonNode.class);
        assertEquals(jsonNode.size(), 5);
        assertEquals(jsonNode.get("scheduler").textValue(), TEST_SCHEDULER);
        assertEquals(jsonNode.get("scheduleType").textValue(), TEST_SCHEDULE_TYPE.getName());
        assertEquals(jsonNode.get("startDateTime").textValue(), TEST_START_DATETIME.toString());
        assertEquals(jsonNode.get("endDateTime").textValue(), TEST_END_DATETIME.toString());

        JsonNode appWhitelistNode = jsonNode.get("appWhitelist");
        assertEquals(appWhitelistNode.size(), 1);
        assertEquals(appWhitelistNode.get(0).textValue(), "test-app");
    }
    
    @Test
    public void deserializeDailySignupsRequest() throws Exception {
        String jsonText = "{\n" +
                "   \"scheduler\":\"test-scheduler\",\n" +
                "   \"scheduleType\":\"DAILY_SIGNUPS\",\n" +
                "   \"startDateTime\":\"2016-10-19T00:00:00.000-07:00\",\n" +
                "   \"endDateTime\":\"2016-10-20T23:59:59.000-07:00\"\n" +
                "}";
        BridgeReporterRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText, BridgeReporterRequest.class);
        assertEquals(request.getScheduleType(), ReportType.DAILY_SIGNUPS);
        assertEquals(request.getStartDateTime().toString(), "2016-10-19T00:00:00.000-07:00");
        assertEquals(request.getEndDateTime().toString(), "2016-10-20T23:59:59.000-07:00");
    }
}
