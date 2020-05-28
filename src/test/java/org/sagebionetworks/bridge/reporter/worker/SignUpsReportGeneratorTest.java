package org.sagebionetworks.bridge.reporter.worker;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.joda.time.DateTime;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.rest.model.AccountStatus;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;

public class SignUpsReportGeneratorTest {
    
    private static final String APP_ID = "test-app";
    private static final DateTime START_DATE = DateTime.parse("2017-06-09T00:00:00.000Z");
    private static final DateTime END_DATE = DateTime.parse("2017-06-09T23:59:59.999Z");
    
    private static final BridgeReporterRequest REQUEST = new BridgeReporterRequest.Builder()
            .withScheduleType(ReportType.DAILY_SIGNUPS)
            .withScheduler("test-scheduler")
            .withStartDateTime(START_DATE)
            .withEndDateTime(END_DATE).build();
    
    @Spy
    SignUpsReportGenerator generator;
    
    @Mock
    private BridgeHelper bridgeHelper;
    
    @BeforeMethod
    public void before() {
        MockitoAnnotations.initMocks(this);
        
        generator = new SignUpsReportGenerator();
        generator.setBridgeHelper(bridgeHelper);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testSharingScopes() throws Exception {
        List<StudyParticipant> participants = new ArrayList<>();
        participants.add((StudyParticipant) new StudyParticipant().status(AccountStatus.ENABLED)
                .sharingScope(SharingScope.NO_SHARING).roles(new ArrayList<>()));
        participants.add((StudyParticipant) new StudyParticipant().status(AccountStatus.ENABLED)
                .sharingScope(SharingScope.SPONSORS_AND_PARTNERS).roles(new ArrayList<>()));
        when(bridgeHelper.getParticipantsForApp(APP_ID, START_DATE, END_DATE)).thenReturn(participants);
        
        Report report = generator.generate(REQUEST, APP_ID);
        
        assertEquals(report.getAppId(), APP_ID);
        assertEquals(report.getReportId(), "test-scheduler-daily-signups-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        
        Map<String,Map<String,Integer>> map = (Map<String,Map<String,Integer>>)report.getData();
        assertEquals(map.get("byStatus").get("enabled"), new Integer(2));
        assertEquals(map.get("bySharing").get("no_sharing"), new Integer(1));
        assertEquals(map.get("bySharing").get("sponsors_and_partners"), new Integer(1));
        
        verify(bridgeHelper).getParticipantsForApp(APP_ID, START_DATE, END_DATE);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testStatuses() throws Exception {
        List<StudyParticipant> participants = new ArrayList<>();
        participants.add((StudyParticipant) new StudyParticipant().status(AccountStatus.ENABLED)
                .sharingScope(SharingScope.NO_SHARING).roles(new ArrayList<>()));
        participants.add((StudyParticipant) new StudyParticipant().status(AccountStatus.ENABLED)
                .sharingScope(SharingScope.NO_SHARING).roles(new ArrayList<>()));
        participants.add((StudyParticipant) new StudyParticipant().status(AccountStatus.ENABLED)
                .sharingScope(SharingScope.ALL_QUALIFIED_RESEARCHERS).roles(new ArrayList<>()));
        participants.add((StudyParticipant) new StudyParticipant().status(AccountStatus.UNVERIFIED)
                .sharingScope(SharingScope.NO_SHARING).roles(new ArrayList<>()));
        when(bridgeHelper.getParticipantsForApp(APP_ID, START_DATE, END_DATE)).thenReturn(participants);
        
        Report report = generator.generate(REQUEST, APP_ID);
        
        assertEquals(report.getAppId(), APP_ID);
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
    
    @SuppressWarnings("unchecked")
    @Test
    public void testParticipantWithRoles() throws Exception {
        List<Role> roles = new ArrayList<>();
        roles.add(Role.DEVELOPER);
        
        List<StudyParticipant> participants = new ArrayList<>();
        participants.add((StudyParticipant) new StudyParticipant().status(AccountStatus.ENABLED)
                .sharingScope(SharingScope.NO_SHARING).roles(roles));
        when(bridgeHelper.getParticipantsForApp(APP_ID, START_DATE, END_DATE)).thenReturn(participants);
        
        Report report = generator.generate(REQUEST, APP_ID);
        
        assertEquals(report.getAppId(), APP_ID);
        assertEquals(report.getReportId(), "test-scheduler-daily-signups-report");
        assertEquals(report.getDate().toString(), "2017-06-09");
        
        Map<String,Map<String,Integer>> map = (Map<String,Map<String,Integer>>)report.getData();
        assertEquals(map.get("byStatus").get("enabled"), new Integer(0));
        assertEquals(map.get("byStatus").get("disabled"), new Integer(0));
        assertEquals(map.get("byStatus").get("unverified"), new Integer(0));
        
        // Only enabled accounts are included in the sharing status information. So there 
        // are only two in this set
        assertEquals(map.get("bySharing").get("no_sharing"), new Integer(0));
        assertEquals(map.get("bySharing").get("all_qualified_researchers"), new Integer(0));
        assertEquals(map.get("bySharing").get("sponsors_and_partners"), new Integer(0));
    }
}
