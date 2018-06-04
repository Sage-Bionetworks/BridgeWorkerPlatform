package org.sagebionetworks.bridge.udd.worker;

import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import org.joda.time.LocalDate;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

@SuppressWarnings("unchecked")
public class BridgeUddRequestTest {
    private static final String START_DATE_STRING = "2015-08-15";
    private static final LocalDate START_DATE = LocalDate.parse(START_DATE_STRING);
    private static final String END_DATE_STRING = "2015-08-19";
    private static final LocalDate END_DATE = LocalDate.parse(END_DATE_STRING);
    private static final String STUDY_ID = "test-study";
    private static final String USER_ID = "test-user-id";

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "studyId must be specified")
    public void nullStudyId() {
        new BridgeUddRequest.Builder().withUserId(USER_ID).withStartDate(START_DATE).withEndDate(END_DATE).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "studyId must be specified")
    public void emptyStudyId() {
        new BridgeUddRequest.Builder().withStudyId("").withUserId(USER_ID).withStartDate(START_DATE)
                .withEndDate(END_DATE).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "userId must be specified")
    public void nullUsername() {
        new BridgeUddRequest.Builder().withStudyId(STUDY_ID).withStartDate(START_DATE).withEndDate(END_DATE).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "userId must be specified")
    public void emptyUsername() {
        new BridgeUddRequest.Builder().withStudyId(STUDY_ID).withUserId("").withStartDate(START_DATE)
                .withEndDate(END_DATE).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "startDate must be specified")
    public void nullStartDate() {
        new BridgeUddRequest.Builder().withStudyId(STUDY_ID).withUserId(USER_ID).withEndDate(END_DATE).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "endDate must be specified")
    public void nullEndDate() {
        new BridgeUddRequest.Builder().withStudyId(STUDY_ID).withUserId(USER_ID).withStartDate(START_DATE).build();
    }

    @Test
    public void startDateBeforeEndDate() {
        BridgeUddRequest request = new BridgeUddRequest.Builder().withStudyId(STUDY_ID).withUserId(USER_ID)
                .withStartDate(START_DATE) .withEndDate(END_DATE).build();
        assertEquals(request.getStudyId(), STUDY_ID);
        assertEquals(request.getUserId(), USER_ID);
        assertEquals(request.getStartDate(), START_DATE);
        assertEquals(request.getEndDate(), END_DATE);
    }

    @Test
    public void startDateSameAsEndDate() {
        BridgeUddRequest request = new BridgeUddRequest.Builder().withStudyId(STUDY_ID).withUserId(USER_ID)
                .withStartDate(END_DATE).withEndDate(END_DATE).build();
        assertEquals(request.getStudyId(), STUDY_ID);
        assertEquals(request.getUserId(), USER_ID);
        assertEquals(request.getStartDate(), END_DATE);
        assertEquals(request.getEndDate(), END_DATE);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp =
            "startDate can't be after endDate")
    public void startDateAfterEndDate() {
        new BridgeUddRequest.Builder().withStudyId(STUDY_ID).withUserId(USER_ID).withStartDate(END_DATE)
                .withEndDate(START_DATE).build();
    }

    @Test
    public void jsonSerializationWithUserId() throws Exception {
        // start with JSON
        String jsonText = "{\n" +
                "   \"studyId\":\"json-study\",\n" +
                "   \"userId\":\"json-user-id\",\n" +
                "   \"startDate\":\"2015-08-03\",\n" +
                "   \"endDate\":\"2015-08-07\"\n" +
                "}";

        // convert to POJO
        BridgeUddRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText, BridgeUddRequest.class);
        assertEquals(request.getStudyId(), "json-study");
        assertEquals(request.getUserId(), "json-user-id");
        assertEquals(request.getStartDate().toString(), "2015-08-03");
        assertEquals(request.getEndDate().toString(), "2015-08-07");

        // convert back to JSON
        JsonNode jsonNode = DefaultObjectMapper.INSTANCE.convertValue(request, JsonNode.class);
        assertEquals(4, jsonNode.size());
        assertEquals(jsonNode.get("studyId").textValue(), "json-study");
        assertEquals(jsonNode.get("userId").textValue(), "json-user-id");
        assertEquals(jsonNode.get("startDate").textValue(), "2015-08-03");
        assertEquals(jsonNode.get("endDate").textValue(), "2015-08-07");
    }
}
