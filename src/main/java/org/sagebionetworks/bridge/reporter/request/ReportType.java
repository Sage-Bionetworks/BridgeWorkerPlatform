package org.sagebionetworks.bridge.reporter.request;

import java.util.EnumSet;

public enum ReportType {
    DAILY("DAILY"), // aka DAILY_UPLOADS 
    WEEKLY("WEEKLY"), // aka WEEKLY_UPLOADS
    DAILY_SIGNUPS("DAILY_SIGNUPS"),
    DAILY_RETENTION("DAILY_RETENTION");
    
    private static final EnumSet<ReportType> UPLOAD_REPORTS = EnumSet.of(DAILY,WEEKLY);

    private final String name;

    ReportType(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public String getSuffix() {
        // Upload reports were initially assumed, account for that here
        if (UPLOAD_REPORTS.contains(this)) {
            return "-" + this.name().toLowerCase() + "-upload-report";
        }
        return "-" + this.name().replaceAll("_", "-").toLowerCase() + "-report";
    }
}
