package org.sagebionetworks.bridge.reporter.worker;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.joda.time.DateTime;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.rest.model.AccountStatus;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;

public class SignUpsReportGeneratorTest {
    
    private static final String STUDY_ID = "test-study";
    private static final DateTime START_DATE = DateTime.parse("2017-06-09T00:00:00.000Z");
    private static final DateTime END_DATE = DateTime.parse("2017-06-09T23:59:59.999Z");

    @SuppressWarnings("unchecked")
    @Test
    public void testSharingScopes() throws Exception {
        BridgeReporterRequest request = new BridgeReporterRequest.Builder()
                .withScheduleType(ReportType.DAILY_SIGNUPS)
                .withScheduler("test-scheduler")
                .withStartDateTime(START_DATE)
                .withEndDateTime(END_DATE).build();
        BridgeHelper bridgeHelper = mock(BridgeHelper.class);

        List<StudyParticipant> participants = new ArrayList<>();
        participants.add((StudyParticipant) new StudyParticipant().status(AccountStatus.ENABLED)
                .sharingScope(SharingScope.NO_SHARING));
        participants.add((StudyParticipant) new StudyParticipant().status(AccountStatus.ENABLED)
                .sharingScope(SharingScope.SPONSORS_AND_PARTNERS));
        when(bridgeHelper.getParticipantsForStudy(STUDY_ID, START_DATE, END_DATE)).thenReturn(participants);
        
        SignUpsReportGenerator generator = new SignUpsReportGenerator();
        generator.setBridgeHelper(bridgeHelper);
        Report report = generator.generate(request, STUDY_ID);
        
        assertEquals(report.getStudyId(), STUDY_ID);
        assertEquals(report.getReportId(), "test-scheduler-daily-signups-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        
        Map<String,Map<String,Integer>> map = (Map<String,Map<String,Integer>>)report.getData();
        assertEquals(map.get("byStatus").get("enabled"), new Integer(2));
        assertEquals(map.get("bySharing").get("no_sharing"), new Integer(1));
        assertEquals(map.get("bySharing").get("sponsors_and_partners"), new Integer(1));
        
        verify(bridgeHelper).getParticipantsForStudy(STUDY_ID, START_DATE, END_DATE);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testStatuses() throws Exception {
        BridgeReporterRequest request = new BridgeReporterRequest.Builder()
                .withScheduleType(ReportType.DAILY_SIGNUPS)
                .withScheduler("test-scheduler")
                .withStartDateTime(START_DATE)
                .withEndDateTime(END_DATE).build();
        BridgeHelper bridgeHelper = mock(BridgeHelper.class);

        List<StudyParticipant> participants = new ArrayList<>();
        participants.add((StudyParticipant) new StudyParticipant().status(AccountStatus.ENABLED)
                .sharingScope(SharingScope.NO_SHARING));
        participants.add((StudyParticipant) new StudyParticipant().status(AccountStatus.ENABLED)
                .sharingScope(SharingScope.NO_SHARING));
        participants.add((StudyParticipant) new StudyParticipant().status(AccountStatus.ENABLED)
                .sharingScope(SharingScope.ALL_QUALIFIED_RESEARCHERS));
        participants.add((StudyParticipant) new StudyParticipant().status(AccountStatus.UNVERIFIED)
                .sharingScope(SharingScope.NO_SHARING));
        when(bridgeHelper.getParticipantsForStudy(STUDY_ID, START_DATE, END_DATE)).thenReturn(participants);
        
        SignUpsReportGenerator generator = new SignUpsReportGenerator();
        generator.setBridgeHelper(bridgeHelper);
        Report report = generator.generate(request, STUDY_ID);
        
        assertEquals(report.getStudyId(), STUDY_ID);
        assertEquals(report.getReportId(), "test-scheduler-daily-signups-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        
        Map<String,Map<String,Integer>> map = (Map<String,Map<String,Integer>>)report.getData();
        assertEquals(map.get("byStatus").get("enabled"), new Integer(3));
        assertEquals(map.get("byStatus").get("disabled"), new Integer(0));
        assertEquals(map.get("byStatus").get("unverified"), new Integer(1));
        
        // Only enabled accounts are included in the sharing status information. So there 
        // are only two in this set
        assertEquals(map.get("bySharing").get("no_sharing"), new Integer(2));
        assertEquals(map.get("bySharing").get("all_qualified_researchers"), new Integer(1));
        assertEquals(map.get("bySharing").get("sponsors_and_partners"), new Integer(0));
    }
}
