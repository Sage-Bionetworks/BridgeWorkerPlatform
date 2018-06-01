package org.sagebionetworks.bridge.reporter.worker;

import static com.google.common.base.Preconditions.checkNotNull;

import org.joda.time.LocalDate;

public class Report {

    private final String studyId;
    private final String reportId;
    private final LocalDate date;
    private final Object data;
    
    private Report(String studyId, String reportId, LocalDate date, Object data) {
        this.studyId = studyId;
        this.reportId = reportId;
        this.date = date;
        this.data = data;
    }

    public String getStudyId() {
        return studyId;
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
        private String studyId;
        private String reportId;
        private LocalDate date;
        private Object data;
        
        public Builder withStudyId(String studyId) {
            this.studyId = studyId;
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
            checkNotNull(studyId);
            checkNotNull(reportId);
            checkNotNull(date);
            checkNotNull(data);
            return new Report(studyId, reportId, date, data);
        }
    }

    @Override
    public String toString() {
        return String.format("Report [studyId=%s, reportId=%s, date=%s, data=%s]",
                studyId, reportId, date, data);
    }
}
