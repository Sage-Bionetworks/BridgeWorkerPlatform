package org.sagebionetworks.bridge.workerPlatform.util;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

/** JSON utility methods. */
public class JsonUtils {
    /**
     * Gets a value from a parent node as a string list. If the value does not exist or is cannot be parsed as a string
     * list, then this returns null.
     */
    public static List<String> asStringList(JsonNode parent, String property) {
        if (parent != null && parent.hasNonNull(property)) {
            JsonNode child = parent.get(property);
            if (child.isArray()) {
                ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
                for (JsonNode subchild : child) {
                    if (!subchild.isTextual()) {
                        // All array elements must be strings. If there's a parse error, return null.
                        return null;
                    }

                    listBuilder.add(subchild.textValue());
                }

                return listBuilder.build();
            }
        }

        // Could not parse as string list.
        return null;
    }

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
