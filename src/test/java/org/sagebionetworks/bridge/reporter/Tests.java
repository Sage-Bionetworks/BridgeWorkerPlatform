package org.sagebionetworks.bridge.reporter;

public class Tests {
    
    public static String unescapeJson(String json) {
        return json.replaceAll("'", "\"");
    }
    
}
