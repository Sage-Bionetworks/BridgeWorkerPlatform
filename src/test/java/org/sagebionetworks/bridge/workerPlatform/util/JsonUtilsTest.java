package org.sagebionetworks.bridge.workerPlatform.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

public class JsonUtilsTest {
    // branch coverage
    @Test
    public void asTextNullParent() {
        assertNull(JsonUtils.asText(null, "any"));
    }

    @DataProvider(name = "asTextProvider")
    public Object[][] asTextProvider() {
        // { [input json text], [expected] }
        // property="prop"
        return new Object[][] {
                { "null", null },
                { "\"string\"", null },
                { "{}", null },
                { "{\"prop\":null}", null },
                { "{\"prop\":42}", null },
                { "{\"prop\":\"\"}", null },
                { "{\"prop\":\"   \"}", null },
                { "{\"prop\":\"value\"}", "value" },
        };
    }

    @Test(dataProvider = "asTextProvider")
    public void asText(String inputJsonText, String expected) throws Exception {
        JsonNode parent = DefaultObjectMapper.INSTANCE.readTree(inputJsonText);
        assertEquals(JsonUtils.asText(parent, "prop"), expected);
    }
}
