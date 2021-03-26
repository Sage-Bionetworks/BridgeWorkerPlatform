package org.sagebionetworks.bridge.participantroster;

import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.testng.annotations.Test;

public class DownloadParticipantRosterRequestTest {
    private static final String APP_ID = "test-app";
    private static final String USER_ID = "test-user-id";
    private static final String PASSWORD = "test-password";

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "appId must be specified")
    public void nullAppId() {
        new DownloadParticipantRosterRequest.Builder().withUserId(USER_ID).withPassword(PASSWORD).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "appId must be specified")
    public void emptyAppId() {
        new DownloadParticipantRosterRequest.Builder().withAppId("").withUserId(USER_ID).withPassword(PASSWORD).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "userId must be specified")
    public void nullUsername() {
        new DownloadParticipantRosterRequest.Builder().withAppId(APP_ID).withPassword(PASSWORD).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "userId must be specified")
    public void emptyUsername() {
        new DownloadParticipantRosterRequest.Builder().withAppId(APP_ID).withUserId("").withPassword(PASSWORD).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "password must be specified")
    public void nullPassword() {
        new DownloadParticipantRosterRequest.Builder().withAppId(APP_ID).withUserId(USER_ID).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "password must be specified")
    public void emptyPassword() {
        new DownloadParticipantRosterRequest.Builder().withAppId(APP_ID).withUserId(USER_ID).withPassword("").build();
    }

    @Test
    public void jsonSerializationWithUserId() throws Exception {
        // start with JSON
        String jsonText = "{\n" +
                "   \"appId\":\"json-app\",\n" +
                "   \"userId\":\"json-user-id\",\n" +
                "   \"password\":\"json-password\"\n" +
                "}";

        // convert to
        DownloadParticipantRosterRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText, DownloadParticipantRosterRequest.class);
        assertEquals(request.getAppId(), "json-app");
        assertEquals(request.getUserId(), "json-user-id");
        assertEquals(request.getPassword(), "json-password");

        JsonNode jsonNode = DefaultObjectMapper.INSTANCE.convertValue(request, JsonNode.class);
        assertEquals(3, jsonNode.size());
        assertEquals(jsonNode.get("appId").textValue(), "json-app");
        assertEquals(jsonNode.get("userId").textValue(), "json-user-id");
        assertEquals(jsonNode.get("password").textValue(), "json-password");
    }    @Test

    public void jsonSerializationWithStudyId() throws Exception {
        // start with JSON
        String jsonText = "{\n" +
                "   \"studyId\":\"json-app\",\n" +
                "   \"userId\":\"json-user-id\",\n" +
                "   \"password\":\"json-password\"\n" +
                "}";

        // convert to
        DownloadParticipantRosterRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText, DownloadParticipantRosterRequest.class);
        assertEquals(request.getAppId(), "json-app");
        assertEquals(request.getUserId(), "json-user-id");
        assertEquals(request.getPassword(), "json-password");

        JsonNode jsonNode = DefaultObjectMapper.INSTANCE.convertValue(request, JsonNode.class);
        assertEquals(3, jsonNode.size());
        assertEquals(jsonNode.get("appId").textValue(), "json-app");
        assertEquals(jsonNode.get("userId").textValue(), "json-user-id");
        assertEquals(jsonNode.get("password").textValue(), "json-password");
    }


}