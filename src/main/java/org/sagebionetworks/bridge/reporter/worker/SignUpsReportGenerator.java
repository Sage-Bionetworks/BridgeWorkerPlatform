package org.sagebionetworks.bridge.reporter.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.rest.model.AccountStatus;
import org.sagebionetworks.bridge.rest.model.ActivityEventList;
import org.sagebionetworks.bridge.rest.model.RequestInfo;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * Generate a report of signups by account statuses.
 *
 */
@Component
public class SignUpsReportGenerator implements ReportGenerator {
    
    private BridgeHelper bridgeHelper;
    
    @Autowired
    @Qualifier("ReporterHelper")
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }
    
    @Override
    public Report generate(BridgeReporterRequest request, String studyId) throws IOException {
        DateTime startDate = request.getStartDateTime();
        DateTime endDate = request.getEndDateTime();
        String scheduler = request.getScheduler();
        ReportType scheduleType = request.getScheduleType();

        String reportId = scheduler + scheduleType.getSuffix();

        Multiset<AccountStatus> statuses = HashMultiset.create();
        Multiset<SharingScope> sharingScopes = HashMultiset.create();

        List<StudyParticipant> participants = bridgeHelper.getParticipantsForStudy(studyId, startDate, endDate);
        for (StudyParticipant participant : participants) {
            statuses.add(participant.getStatus());
            System.out.println(participant.getCreatedOn() + " : " + participant.getStatus() + " : " + participant.getEmail());
            
            // Accounts that aren't enabled do not have interesting sharing statuses. We'd like to count these
            // for consented accounts, but this isn't easy to do.
            if (participant.getStatus() == AccountStatus.ENABLED) {
                sharingScopes.add(participant.getSharingScope());    
            }
        }
        
        // Multiset doesn't record zeroes, so add these first for all enum values, then overwrite
        // (so JSON includes properties for all enumerations, even if missing).
        Map<String, Integer> statusData = new HashMap<>();
        for (AccountStatus status : AccountStatus.values()) {
            statusData.put(status.name().toLowerCase(), 0);
        }
        for (AccountStatus status : statuses) {
            statusData.put(status.name().toLowerCase(), statuses.count(status));
        }
        Map<String, Integer> sharingData = new HashMap<>();
        for (SharingScope scope : SharingScope.values()) {
            sharingData.put(scope.name().toLowerCase(), 0);
        }
        for (SharingScope scope : sharingScopes) {
            sharingData.put(scope.name().toLowerCase(), sharingScopes.count(scope));
        }
        
        Map<String, Map<String,Integer>> reportData = new HashMap<>();
        reportData.put("bySharing", sharingData);
        reportData.put("byStatus", statusData);

        	
        // Total number of participants: last_sign_in vs days_in_study
        Map<StudyParticipant, RequestInfo> requestInfos = bridgeHelper.getRequestInfoForParticipant(studyId, participants);
        List<Integer> signInData = new ArrayList<>();
        for (Entry<StudyParticipant, RequestInfo> entry : requestInfos.entrySet()) {
        	if (entry.getValue().getSignedInOn() != null) { // Has signed in
        		System.out.println("Participant: " + entry.getKey().getEmail());
        		System.out.println("Created on: " + entry.getKey().getCreatedOn().toLocalDate() + " Signed in on: " + entry.getValue().getSignedInOn().toLocalDate());
        		int days = Days.daysBetween(entry.getKey().getCreatedOn().toLocalDate(), entry.getValue().getSignedInOn().toLocalDate()).getDays();
        		System.out.println("Days between: " + days);
        		while (signInData.size() < (days + 1)) {
            		signInData.add(0);
            	}
        		signInData.set(days, signInData.get(days) + 1);
        	}
        }
        System.out.println("Sign In Data: " + signInData.toString());
        
        // Total number of participants: last_uploaded_on vs days_in_study
        Map<RequestInfo, ActivityEventList> activityEventList = bridgeHelper.getActivityEventForParticipant(studyId, participants);
        List<Integer> uploadedOnData = new ArrayList<>();
        for (Entry<RequestInfo, ActivityEventList> entry : activityEventList.entrySet()) {
        	if (entry.getKey().getSignedInOn() != null) { // Has signed in
        		for (int i = 0; i < entry.getValue().getItems().size(); i++) {
    	    		if (entry.getValue().getItems().get(i).getEventId().equals("activities_retrieved")) {
    	    			System.out.println("Activity Retrieved  on: " + entry.getValue().getItems().get(i).getTimestamp().toDateTime().toLocalDate() + " Uploaded on: " + entry.getKey().getUploadedOn().toLocalDate());
    	    			int days = Days.daysBetween(entry.getValue().getItems().get(i).getTimestamp().toDateTime().toLocalDate(), entry.getKey().getUploadedOn().toLocalDate()).getDays();
    	        		System.out.println("Days between: " + days);
    	        		while (uploadedOnData.size() < (days + 1)) {
    	    				uploadedOnData.add(0);
    	            	}
    	    			uploadedOnData.set(days, uploadedOnData.get(days) + 1);
    	    		}
        		}
        	}
        }
        System.out.println("Upload On Data: " + uploadedOnData.toString());
//        reportData.put("bySignIn", signInData);
//        reportData.put("byUploadedOn", uploadedOnData);
        
        return new Report.Builder().withStudyId(studyId).withReportId(reportId).withDate(startDate.toLocalDate())
                .withReportData(reportData).build();
    }
}
