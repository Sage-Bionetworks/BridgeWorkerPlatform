package org.sagebionetworks.bridge.reporter.worker;

import static org.testng.Assert.assertEquals;

import org.joda.time.LocalDate;
import org.testng.annotations.Test;

public class ReportTest {
    
    @Test
    public void ReportWorks() {
        LocalDate date = LocalDate.parse("2017-05-30");
        Object data = new Object();
        
        Report report = new Report.Builder()
                .withStudyId("studyId")
                .withReportId("reportId")
                .withDate(date)
                .withReportData(data).build();
        assertEquals(report.getStudyId(), "studyId");
        assertEquals(report.getReportId(), "reportId");
        assertEquals(report.getDate(), date);
        assertEquals(report.getData(), data);
    }

}
