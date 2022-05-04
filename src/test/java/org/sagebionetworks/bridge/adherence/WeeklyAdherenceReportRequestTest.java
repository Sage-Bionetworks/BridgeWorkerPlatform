package org.sagebionetworks.bridge.adherence;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;

public class WeeklyAdherenceReportRequestTest {
    
    @Test
    public void testAllConfiguration() throws JsonMappingException, JsonProcessingException {
        String json = "{"   
                +"  \"defaultZoneId\": \"America/Denver\", "
                +"  \"selectedStudies\": {\"foo\": [\"study1\", \"study2\"]}, "
                +"  \"reportingHours\": [5, 12, 15] "
                + "}";

        WeeklyAdherenceReportRequest request = new ObjectMapper().readValue(json, WeeklyAdherenceReportRequest.class);
        assertEquals(request.getDefaultZoneId(), "America/Denver");
        assertEquals(request.getSelectedStudies().get("foo"), ImmutableSet.of("study1", "study2"));
        assertEquals(request.getReportingHours(), ImmutableSet.of(5, 12, 15));
        
        // Let's test the serialization because we want to use it in the integration tests
        JsonNode node = new ObjectMapper().valueToTree(request);
        assertEquals(node.get("defaultZoneId").textValue(), "America/Denver");
        assertEquals(node.get("selectedStudies").get("foo").get(0).textValue(), "study1");
        assertEquals(node.get("selectedStudies").get("foo").get(1).textValue(), "study2");
        assertEquals(node.get("reportingHours").get(0).intValue(), 5);
        assertEquals(node.get("reportingHours").get(1).intValue(), 12);
        assertEquals(node.get("reportingHours").get(2).intValue(), 15);
    }

    @Test
    public void testNoConfiguration() throws JsonMappingException, JsonProcessingException {
        WeeklyAdherenceReportRequest request = new ObjectMapper().readValue("{}", WeeklyAdherenceReportRequest.class);
        assertEquals(request.getDefaultZoneId(), "America/Chicago");
        assertEquals(request.getReportingHours(), ImmutableSet.of(4, 11));
        assertTrue(request.getSelectedStudies().isEmpty()); // ie everything
    }
}
