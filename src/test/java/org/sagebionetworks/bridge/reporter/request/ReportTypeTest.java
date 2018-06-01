package org.sagebionetworks.bridge.reporter.request;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class ReportTypeTest {
    // branch coverage test to satisfy jacoco
    @Test
    public void valueOf() {
        assertEquals(ReportType.valueOf("DAILY"), ReportType.DAILY);
        assertEquals(ReportType.valueOf("WEEKLY"), ReportType.WEEKLY);
        assertEquals(ReportType.valueOf("DAILY_SIGNUPS"), ReportType.DAILY_SIGNUPS);
    }
    
    @Test
    public void test() {
        assertEquals(ReportType.DAILY.getSuffix(), "-daily-upload-report");
        assertEquals(ReportType.WEEKLY.getSuffix(), "-weekly-upload-report");
        assertEquals(ReportType.DAILY_SIGNUPS.getSuffix(), "-daily-signups-report");
    }
}
