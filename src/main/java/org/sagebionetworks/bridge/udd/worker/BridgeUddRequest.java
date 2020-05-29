package org.sagebionetworks.bridge.udd.worker;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.joda.deser.LocalDateDeserializer;
import com.google.common.base.Strings;
import org.joda.time.LocalDate;

import org.sagebionetworks.bridge.json.LocalDateToStringSerializer;

/** Represents a request to the Bridge User Data Download Service. */
@JsonDeserialize(builder = BridgeUddRequest.Builder.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class BridgeUddRequest {
    private final String appId;
    private final String userId;
    private final LocalDate startDate;
    private final LocalDate endDate;

    /** Private constructor. To construct, use builder. */
    private BridgeUddRequest(String appId, String userId, LocalDate startDate, LocalDate endDate) {
        this.appId = appId;
        this.userId = userId;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    /** ID of the app to get user data from. */
    public String getAppId() {
        return appId;
    }

    /** ID of the user requesting their data. */
    public String getUserId() {
        return userId;
    }

    /** Start date (inclusive) of data to fetch. */
    @JsonSerialize(using = LocalDateToStringSerializer.class)
    public LocalDate getStartDate() {
        return startDate;
    }

    /** End date (inclusive) of data to fetch. */
    @JsonSerialize(using = LocalDateToStringSerializer.class)
    public LocalDate getEndDate() {
        return endDate;
    }

    /** Bridge-UDD request builder. */
    public static class Builder {
        private String appId;
        private String userId;
        private LocalDate startDate;
        private LocalDate endDate;

        /** @see BridgeUddRequest#getAppId */
        @JsonAlias("studyId")
        public Builder withAppId(String appId) {
            this.appId = appId;
            return this;
        }

        /** @see BridgeUddRequest#getUserId */
        public Builder withUserId(String userId) {
            this.userId = userId;
            return this;
        }

        /** @see BridgeUddRequest#getStartDate */
        @JsonDeserialize(using = LocalDateDeserializer.class)
        public Builder withStartDate(LocalDate startDate) {
            this.startDate = startDate;
            return this;
        }

        /** @see BridgeUddRequest#getEndDate */
        @JsonDeserialize(using = LocalDateDeserializer.class)
        public Builder withEndDate(LocalDate endDate) {
            this.endDate = endDate;
            return this;
        }

        /**
         * Builds a BridgeUddRequest and validates that all fields are specified and that start date isn't after end
         * date.
         */
        public BridgeUddRequest build() {
            if (Strings.isNullOrEmpty(appId)) {
                throw new IllegalStateException("appId must be specified");
            }

            if (Strings.isNullOrEmpty(userId)) {
                throw new IllegalStateException("userId must be specified");
            }

            if (startDate == null) {
                throw new IllegalStateException("startDate must be specified");
            }

            if (endDate == null) {
                throw new IllegalStateException("endDate must be specified");
            }

            if (startDate.isAfter(endDate)) {
                throw new IllegalStateException("startDate can't be after endDate");
            }

            return new BridgeUddRequest(appId, userId, startDate, endDate);
        }
    }
}
