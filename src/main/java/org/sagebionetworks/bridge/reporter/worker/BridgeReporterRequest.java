package org.sagebionetworks.bridge.reporter.worker;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.sagebionetworks.bridge.json.DateTimeDeserializer;
import org.sagebionetworks.bridge.json.DateTimeToStringSerializer;
import org.sagebionetworks.bridge.reporter.request.ReportType;

/** Represents a request to the Bridge Reporting Service. */
@JsonDeserialize(builder = BridgeReporterRequest.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BridgeReporterRequest {
    private final DateTime startDateTime;
    private final DateTime endDateTime;
    private final String scheduler;
    private final List<String> appWhitelist;
    private final ReportType reportType;

    /** Private constructor. To build, use Builder. */
    private BridgeReporterRequest(DateTime startDateTime, DateTime endDateTime, String scheduler,
            List<String> appWhitelist, ReportType scheduleType) {
        this.startDateTime = startDateTime;
        this.endDateTime = endDateTime;
        this.scheduler = scheduler;
        this.appWhitelist = appWhitelist;
        this.reportType = scheduleType;
    }

    /** Start time (inclusive) of the report. */
    @JsonSerialize(using = DateTimeToStringSerializer.class)
    public DateTime getStartDateTime() {
        return this.startDateTime;
    }

    /** End time (inclusive) of the report. */
    @JsonSerialize(using = DateTimeToStringSerializer.class)
    public DateTime getEndDateTime() {
        return this.endDateTime;
    }

    /** Scheduler name, synonymous with report name. */
    public String getScheduler() {
        return this.scheduler;
    }

    /**
     * List of apps to generate reports for. If no app is specified (empty list), this will generate reports for
     * all apps. Will never be null.
     */
    public List<String> getAppWhitelist() {
        return appWhitelist;
    }

    /** Report type, used to select the report generator. */
    public ReportType getScheduleType() {
        return this.reportType;
    }

    /** Bridge-Reporter request builder */
    public static class Builder {
        private DateTime startDateTime;
        private DateTime endDateTime;
        private String scheduler;
        private List<String> appWhitelist;
        private ReportType reportType;

        /** @see BridgeReporterRequest#getStartDateTime */
        @JsonDeserialize(using = DateTimeDeserializer.class)
        public Builder withStartDateTime(DateTime startDateTime) {
            this.startDateTime = startDateTime;
            return this;
        }

        /** @see BridgeReporterRequest#getEndDateTime */
        @JsonDeserialize(using = DateTimeDeserializer.class)
        public Builder withEndDateTime(DateTime endDateTime) {
            this.endDateTime = endDateTime;
            return this;
        }

        /** @see BridgeReporterRequest#getScheduler */
        public Builder withScheduler(String scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        /** @see BridgeReporterRequest#getAppWhitelist */
        @JsonAlias("studyWhitelist")
        public Builder withAppWhitelist(List<String> appWhitelist) {
            this.appWhitelist = appWhitelist;
            return this;
        }

        /** @see BridgeReporterRequest#getScheduleType */
        public Builder withScheduleType(ReportType reportType) {
            this.reportType = reportType;
            return this;
        }

        /** Builds a request. */
        public BridgeReporterRequest build() {
            if (Strings.isNullOrEmpty(scheduler)) {
                throw new IllegalStateException("scheduler must be specified.");
            }

            if (reportType == null) {
                throw new IllegalStateException("scheduleType must be specified.");
            }

            if (startDateTime == null) {
                throw new IllegalStateException("startDateTime must be specified.");
            }

            if (endDateTime == null) {
                throw new IllegalStateException("endDateTime must be specified.");
            }

            if (startDateTime.isAfter(endDateTime)) {
                throw new IllegalStateException("startDateTime can't be after endDateTime.");
            }

            // Make an immutable copy of the whitelist. If it's null, make an immutable empty list.
            List<String> appWhitelistCopy;
            if (appWhitelist != null) {
                appWhitelistCopy = ImmutableList.copyOf(appWhitelist);
            } else {
                appWhitelistCopy = ImmutableList.of();
            }

            return new BridgeReporterRequest(startDateTime, endDateTime, scheduler, appWhitelistCopy, reportType);
        }
    }
}
