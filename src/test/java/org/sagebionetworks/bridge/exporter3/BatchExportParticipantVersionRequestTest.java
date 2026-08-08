package org.sagebionetworks.bridge.exporter3;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

public class BatchExportParticipantVersionRequestTest {
    @Test
    public void deserialize() throws Exception {
        // Start with JSON text.
        String jsonText = "{\n" +
                "   \"appId\":\"test-app\",\n" +
                "   \"participantVersionIdentifiers\":[\n" +
                "       {\n" +
                "           \"healthCode\":\"test-health-code-1\",\n" +
                "           \"participantVersion\":1\n" +
                "       },\n" +
                "       {\n" +
                "           \"healthCode\":\"test-health-code-2\",\n" +
                "           \"participantVersion\":2\n" +
                "       }\n" +
                "   ]\n" +
                "}";

        // Convert to Java object.
        BatchExportParticipantVersionRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText,
                BatchExportParticipantVersionRequest.class);
        assertEquals(request.getAppId(), "test-app");

        List<BatchExportParticipantVersionRequest.ParticipantVersionIdentifier> identifierList =
                request.getParticipantVersionIdentifiers();
        assertEquals(identifierList.size(), 2);

        BatchExportParticipantVersionRequest.ParticipantVersionIdentifier identifier1 = identifierList.get(0);
        assertEquals(identifier1.getHealthCode(), "test-health-code-1");
        assertEquals(identifier1.getParticipantVersion(), 1);

        BatchExportParticipantVersionRequest.ParticipantVersionIdentifier identifier2 = identifierList.get(1);
        assertEquals(identifier2.getHealthCode(), "test-health-code-2");
        assertEquals(identifier2.getParticipantVersion(), 2);
    }

    // branch coverage
    @Test
    public void noIdentifiers() throws Exception {
        // Start with JSON text.
        String jsonText = "{\n" +
                "   \"appId\":\"test-app\"\n" +
                "}";

        // Convert to Java object.
        BatchExportParticipantVersionRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText,
                BatchExportParticipantVersionRequest.class);
        assertEquals(request.getAppId(), "test-app");
        assertTrue(request.getParticipantVersionIdentifiers().isEmpty());
    }

    // branch coverage
    @Test
    public void identifiersNull() throws Exception {
        // Start with JSON text.
        String jsonText = "{\n" +
                "   \"appId\":\"test-app\",\n" +
                "   \"participantVersionIdentifiers\":null\n" +
                "}";

        // Convert to Java object.
        BatchExportParticipantVersionRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText,
                BatchExportParticipantVersionRequest.class);
        assertEquals(request.getAppId(), "test-app");
        assertTrue(request.getParticipantVersionIdentifiers().isEmpty());
    }
}
