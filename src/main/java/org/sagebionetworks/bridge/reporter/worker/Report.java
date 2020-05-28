package org.sagebionetworks.bridge.reporter.worker;

import static com.google.common.base.Preconditions.checkNotNull;

import org.joda.time.LocalDate;

public class Report {

    private final String appId;
    private final String reportId;
    private final LocalDate date;
    private final Object data;
    
    private Report(String appId, String reportId, LocalDate date, Object data) {
        this.appId = appId;
        this.reportId = reportId;
        this.date = date;
        this.data = data;
    }

    public String getAppId() {
        return appId;
    }

    public String getReportId() {
        return reportId;
    }

    public LocalDate getDate() {
        return date;
    }

    public Object getData() {
        return data;
    }

    public static class Builder {
        private String appId;
        private String reportId;
        private LocalDate date;
        private Object data;
        
        public Builder withAppId(String appId) {
            this.appId = appId;
            return this;
        }
        public Builder withReportId(String reportId) {
            this.reportId = reportId;
            return this;
        }
        public Builder withDate(LocalDate date) {
            this.date = date;
            return this;
        }
        public Builder withReportData(Object reportData) {
            this.data = reportData;
            return this;
        }
        public Report build() {
            checkNotNull(appId);
            checkNotNull(reportId);
            checkNotNull(date);
            checkNotNull(data);
            return new Report(appId, reportId, date, data);
        }
    }

    @Override
    public String toString() {
        return String.format("Report [appId=%s, reportId=%s, date=%s, data=%s]",
                appId, reportId, date, data);
    }
}
