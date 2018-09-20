package org.sagebionetworks.bridge.workerPlatform.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;

/** JSON utility methods. */
public class JsonUtils {
    /**
     * Gets a value from a parent node as a string. If the value does not exist or is not a string or is a blank
     * string, this returns null.
     */
    public static String asText(JsonNode parent, String property) {
        if (parent != null && parent.hasNonNull(property)) {
            JsonNode child = parent.get(property);
            if (child.isTextual()) {
                String value = child.textValue();
                if (StringUtils.isNotBlank(value)) {
                    return value;
                }
            }
        }

        return null;
    }
}
