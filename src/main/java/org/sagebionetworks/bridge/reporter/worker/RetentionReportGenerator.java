package org.sagebionetworks.bridge.reporter.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.util.concurrent.RateLimiter;

import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.ActivityEventList;
import org.sagebionetworks.bridge.rest.model.RequestInfo;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generate a report of sign ins and uploads by days in study.
 *
 */
@Component
public class RetentionReportGenerator implements ReportGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(RetentionReportGenerator.class);
    private final RateLimiter perUserRateLimiter = RateLimiter.create(100.0);
    private BridgeHelper bridgeHelper;
    
    @Autowired
    @Qualifier("ReporterHelper")
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    @Override
    public Report generate(BridgeReporterRequest request, String studyId) throws IOException {

        DateTime startDate = request.getStartDateTime();
        ReportType scheduleType = request.getScheduleType();
        String reportId = scheduleType.getSuffix();

        Iterator<AccountSummary> accountSummaryIter = bridgeHelper.getAllAccountSummaries(studyId);

        List<Integer> signInData = new ArrayList<>();
        List<Integer> uploadedOnData = new ArrayList<>();
        
        while (accountSummaryIter.hasNext()) {
            // Rate limit
            perUserRateLimiter.acquire();
            
            AccountSummary accountSummary = accountSummaryIter.next();
            try {
                StudyParticipant studyParticipant = bridgeHelper.getStudyPartcipant(studyId, accountSummary.getId());
                if (!studyParticipant.getRoles().isEmpty()) {
                    continue;
                }
                
                ActivityEventList activityEventList = bridgeHelper.getActivityEventForParticipant(
                        studyId, accountSummary.getId());
                DateTime studyStartDate = null;
                
                for (int i = 0; i < activityEventList.getItems().size(); i++) {
                    if (activityEventList.getItems().get(i).getEventId().equals("study_start_date")) {
                        studyStartDate = activityEventList.getItems().get(i).getTimestamp();
                        break;
                    }
                }
                
                if (studyStartDate == null) {
                    LOG.error("No study_state_date event for id=" + accountSummary.getId());
                    continue;
                }
                
                RequestInfo requestInfo = bridgeHelper.getRequestInfoForParticipant(
                        studyId, accountSummary.getId());
                if (requestInfo.getSignedInOn() != null) {
                    int sign_in_days = Days.daysBetween(studyStartDate.withZone(DateTimeZone.UTC), 
                            requestInfo.getSignedInOn().withZone(DateTimeZone.UTC)).getDays();
                    if (sign_in_days < 0) {
                        LOG.error("study_state_date is negative for id=" + accountSummary.getId());
                        continue;
                    }
                    
                    while (signInData.size() < (sign_in_days + 1)) {
                        signInData.add(0);
                    }
                    signInData.set(sign_in_days, signInData.get(sign_in_days) + 1);
                }
                if (requestInfo.getUploadedOn() != null) {
                    int upload_on_days = Days.daysBetween(studyStartDate.withZone(DateTimeZone.UTC), 
                            requestInfo.getUploadedOn().withZone(DateTimeZone.UTC)).getDays();
                    if (upload_on_days < 0) {
                        LOG.error("upload_on_days is negative for id=" + accountSummary.getId());
                        continue;
                    }
                    
                    while (uploadedOnData.size() < (upload_on_days + 1)) {
                        uploadedOnData.add(0);
                    }
                    uploadedOnData.set(upload_on_days, uploadedOnData.get(upload_on_days) + 1);
                }
            } catch (Exception ex) {
                LOG.error("Error getting data for id " + accountSummary.getId() + ": " + ex.getMessage(), ex);
            }
        }
        
        for (int i = signInData.size() - 2; i >= 0; i--) {
            signInData.set(i, signInData.get(i) + signInData.get(i + 1));
        }
        for (int i = uploadedOnData.size() - 2; i >= 0; i--) {
            uploadedOnData.set(i, uploadedOnData.get(i) + uploadedOnData.get(i + 1));
        }
        
        Map<String, List<Integer>> reportData = new HashMap<>();
        reportData.put("bySignIn", signInData);
        reportData.put("byUploadedOn", uploadedOnData);
        
        return new Report.Builder().withStudyId(studyId).withReportId(reportId).withDate(startDate.toLocalDate())
                .withReportData(reportData).build();
    }
}
