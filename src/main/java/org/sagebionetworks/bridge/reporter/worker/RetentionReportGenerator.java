package org.sagebionetworks.bridge.reporter.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.sagebionetworks.bridge.reporter.helper.BridgeHelper;
import org.sagebionetworks.bridge.reporter.request.ReportType;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.ActivityEventList;
import org.sagebionetworks.bridge.rest.model.RequestInfo;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;

/**
 * Generate a report of sign ins and uploads by days in study.
 *
 */
@Component
public class RetentionReportGenerator implements ReportGenerator {

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
        String reportId = scheduleType.getSuffix();

        Iterator<AccountSummary> accountSummaryIter = bridgeHelper.getAllAccountSummaries(studyId);

        List<Integer> signInData = new ArrayList<>();
        List<Integer> uploadedOnData = new ArrayList<>();
        while (accountSummaryIter.hasNext()) {
            AccountSummary accountSummary = accountSummaryIter.next();
            StudyParticipant studyParticipant = bridgeHelper.getStudyPartcipant(studyId, accountSummary.getId());
            RequestInfo requestInfo = bridgeHelper.getRequestInfoForParticipant(studyId, accountSummary.getId());
            ActivityEventList activityEventList = bridgeHelper.getActivityEventForParticipant(studyId, accountSummary.getId());
            DateTime studyStartDate = studyParticipant.getCreatedOn();
            
            for (int i = 0; i < activityEventList.getItems().size(); i++) {
                if (activityEventList.getItems().get(i).getEventId().equals("study_start_date")) {
                    studyStartDate = activityEventList.getItems().get(i).getTimestamp();
                }
            }
            
            if (requestInfo.getSignedInOn() != null) {
                int sign_in_days = Days.daysBetween(studyStartDate.toLocalDate(), requestInfo.getSignedInOn().toLocalDate()).getDays();
                while (signInData.size() < (sign_in_days + 1)) {
                    signInData.add(0);
                }
                signInData.set(sign_in_days, signInData.get(sign_in_days) + 1);
            }
            if (requestInfo.getUploadedOn() != null) {
                int upload_on_days = Days.daysBetween(studyStartDate.toLocalDate(), requestInfo.getUploadedOn().toLocalDate()).getDays();
                while (uploadedOnData.size() < (upload_on_days + 1)) {
                    uploadedOnData.add(0);
                }
                uploadedOnData.set(upload_on_days, uploadedOnData.get(upload_on_days) + 1);
            }
        }

        Map<String, List<Integer>> reportData = new HashMap<>();
        reportData.put("bySignIn", signInData);
        reportData.put("byUploadedOn", uploadedOnData);
        
        return new Report.Builder().withStudyId(studyId).withReportId(reportId).withDate(startDate.toLocalDate())
                .withReportData(reportData).build();
    }
}
