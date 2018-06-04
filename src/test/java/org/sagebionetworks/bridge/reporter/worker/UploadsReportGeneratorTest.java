package org.sagebionetworks.bridge.reporter.worker;

import static org.testng.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.rest.model.UploadStatus;

public class UploadsReportGeneratorTest {
    
    private static final String STUDY_ID = "studyId";
    private static final DateTime START_DATE = DateTime.parse("2017-06-09T00:00:00.000-07:00");
    private static final DateTime END_DATE = DateTime.parse("2017-06-09T23:59:59.999-07:00");

    @SuppressWarnings("unchecked")
    @Test
    public void test() throws Exception {
        BridgeReporterRequest request = new BridgeReporterRequest.Builder()
                .withScheduleType(ReportType.DAILY_SIGNUPS)
                .withScheduler("test-scheduler")
                .withStartDateTime(START_DATE)
                .withEndDateTime(END_DATE).build();
        BridgeHelper bridgeHelper = mock(BridgeHelper.class);

        List<Upload> uploads = new ArrayList<>();
        uploads.add(new Upload().recordId("record1").status(UploadStatus.SUCCEEDED));
        uploads.add(new Upload().recordId("record2").status(UploadStatus.REQUESTED));
        when(bridgeHelper.getUploadsForStudy(STUDY_ID, START_DATE, END_DATE)).thenReturn(uploads);
        
        UploadsReportGenerator generator = new UploadsReportGenerator();
        generator.setBridgeHelper(bridgeHelper);
        Report report = generator.generate(request, STUDY_ID);
        
        assertEquals(report.getStudyId(), STUDY_ID);
        assertEquals(report.getReportId(), "test-scheduler-daily-signups-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        Map<String, Integer> map = (Map<String, Integer>)report.getData();
        assertEquals(map.get("requested"), new Integer(1));
        assertEquals(map.get("succeeded"), new Integer(1));
        
        verify(bridgeHelper).getUploadsForStudy(STUDY_ID, START_DATE, END_DATE);
    }
}
