package org.sagebionetworks.bridge.exporter3;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

public class Exporter3RequestTest {
    @Test
    public void deserialize() throws Exception {
        // Start with JSON text.
        String jsonText = "{\n" +
                "   \"appId\":\"test-app\",\n" +
                "   \"recordId\":\"test-record\"\n" +
                "}";

        // Convert to Java object.
        Exporter3Request request = DefaultObjectMapper.INSTANCE.readValue(jsonText, Exporter3Request.class);
        assertEquals(request.getAppId(), "test-app");
        assertEquals(request.getRecordId(), "test-record");
    }
}
