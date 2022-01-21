package org.sagebionetworks.bridge.exporter3;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

public class Ex3ParticipantVersionRequestTest {
    @Test
    public void deserialize() throws Exception {
        // Start with JSON text.
        String jsonText = "{\n" +
                "   \"appId\":\"test-app\",\n" +
                "   \"healthCode\":\"test-health-code\",\n" +
                "   \"participantVersion\":42\n" +
                "}";

        // Convert to Java object.
        Ex3ParticipantVersionRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText,
                Ex3ParticipantVersionRequest.class);
        assertEquals(request.getAppId(), "test-app");
        assertEquals(request.getHealthCode(), "test-health-code");
        assertEquals(request.getParticipantVersion(), 42);
    }
}
