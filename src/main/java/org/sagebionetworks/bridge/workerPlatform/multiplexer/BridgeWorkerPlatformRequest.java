package org.sagebionetworks.bridge.workerPlatform.multiplexer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/** Represents a request to the Bridge Reporting Service. */
@JsonDeserialize(builder = BridgeWorkerPlatformRequest.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BridgeWorkerPlatformRequest {
    private final String service;
    private final JsonNode body;

    private BridgeWorkerPlatformRequest(String service, JsonNode body) {
        this.service = service;
        this.body = body;
    }

    public String getService() {
        return this.service;
    }

    public JsonNode getBody() {
        return this.body;
    }
    /*
    Bridge-WorkerPlatform request builder
     */
    public static class Builder {
        private String service;
        private JsonNode body;

        public Builder withService(String service) {
            this.service = service;
            return this;
        }

        public Builder withBody(JsonNode body) {
            this.body = body;
            return this;
        }

        public BridgeWorkerPlatformRequest build() {
            if (service == null) {
                throw new IllegalStateException("service must be specified.");
            }

            if (body == null) {
                throw new IllegalStateException("body must be specified.");
            }

            return new BridgeWorkerPlatformRequest(service, body);
        }
    }
}
