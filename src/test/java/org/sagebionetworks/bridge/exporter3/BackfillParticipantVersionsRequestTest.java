package org.sagebionetworks.bridge.exporter3;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

public class BackfillParticipantVersionsRequestTest {
    @Test
    public void deserialize() throws Exception {
        // Start with JSON text.
        String jsonText = "{\n" +
                "   \"appId\":\"test-app\",\n" +
                "   \"s3Key\":\"my-healthcode-list\"\n" +
                "}";

        // Convert to Java object.
        BackfillParticipantVersionsRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText,
                BackfillParticipantVersionsRequest.class);
        assertEquals(request.getAppId(), "test-app");
        assertEquals(request.getS3Key(), "my-healthcode-list");
    }
}
