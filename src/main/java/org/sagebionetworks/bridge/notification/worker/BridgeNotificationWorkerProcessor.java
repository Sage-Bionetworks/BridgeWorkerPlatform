package org.sagebionetworks.bridge.notification.worker;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.RateLimiter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.notification.helper.BridgeHelper;
import org.sagebionetworks.bridge.notification.helper.DynamoHelper;
import org.sagebionetworks.bridge.notification.helper.TemplateVariableHelper;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.ActivityEvent;
import org.sagebionetworks.bridge.rest.model.ScheduleStatus;
import org.sagebionetworks.bridge.rest.model.ScheduledActivity;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.UserConsentHistory;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.time.DateUtils;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;

/** Worker that sends notifications when users do not engage with the study burst. */
@Component("ActivityNotificationWorker")
public class BridgeNotificationWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeNotificationWorkerProcessor.class);

    // If there are a lot of users, write log messages regularly so we know the worker is still running.
    private static final int REPORTING_INTERVAL = 250;

    private static final int MIN_TIMEZONE_OFFSET_MILLIS = -11 * 60 * 60 * 1000;
    private static final int MAX_TIMEZONE_OFFSET_MILLIS = -1 * 60 * 60 * 1000;
    static final String REQUEST_PARAM_DATE = "date";
    static final String REQUEST_PARAM_STUDY_ID = "studyId";
    static final String REQUEST_PARAM_USER_LIST = "userList";
    static final String REQUEST_PARAM_TAG = "tag";

    private final RateLimiter perUserRateLimiter = RateLimiter.create(1.0);

    private BridgeHelper bridgeHelper;
    private DynamoHelper dynamoHelper;
    private Random rng = new Random();
    private TemplateVariableHelper templateVariableHelper;

    /** Bridge helper. */
    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    /** DynamoDB Helper. */
    @Autowired
    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    /** Set rate limit, in users per second. This is primarily to allow unit tests to run without being throttled. */
    public final void setPerUserRateLimit(double rate) {
        perUserRateLimiter.setRate(rate);
    }

    /** Allows mocking of the RNG. */
    public void setRng(Random rng) {
        this.rng = rng;
    }

    /** Helper class that resolves template variables in SMS strings. */
    @Autowired
    public final void setTemplateVariableHelper(TemplateVariableHelper templateVariableHelper) {
        this.templateVariableHelper = templateVariableHelper;
    }

    /** Main entry point into the Notification Worker. */
    @Override
    public void accept(JsonNode jsonNode) throws PollSqsWorkerBadRequestException {
        // Get request args
        // studyId
        JsonNode studyIdNode = jsonNode.get(REQUEST_PARAM_STUDY_ID);
        if (studyIdNode == null || studyIdNode.isNull()) {
            throw new PollSqsWorkerBadRequestException("studyId must be specified");
        }
        if (!studyIdNode.isTextual()) {
            throw new PollSqsWorkerBadRequestException("studyId must be a string");
        }
        String studyId = studyIdNode.textValue();

        // date
        JsonNode dateNode = jsonNode.get(REQUEST_PARAM_DATE);
        if (dateNode == null || dateNode.isNull()) {
            throw new PollSqsWorkerBadRequestException("date must be specified");
        }
        if (!dateNode.isTextual()) {
            throw new PollSqsWorkerBadRequestException("date must be a string");
        }

        String dateString = dateNode.textValue();
        LocalDate date;
        try {
            date = LocalDate.parse(dateString);
        } catch (IllegalArgumentException ex) {
            throw new PollSqsWorkerBadRequestException("date must be in the format YYYY-MM-DD");
        }

        // tag
        JsonNode tagNode = jsonNode.get(REQUEST_PARAM_TAG);
        String tag = null;
        if (tagNode != null && !tagNode.isNull()) {
            tag = tagNode.textValue();
        }

        LOG.info("Received request for study=" + studyId + ", date=" + dateString + ", tag=" + tag);

        // Iterate over each user. All we care about is the userID, so transform the iterator.
        Iterator<String> userIdIterator;
        JsonNode userListNode = jsonNode.get(REQUEST_PARAM_USER_LIST);
        if (userListNode != null) {
            userIdIterator = Iterators.transform(userListNode.elements(), JsonNode::textValue);
            LOG.info("Custom user list received, " + userListNode.size() + " users");
        } else {
            userIdIterator = Iterators.transform(bridgeHelper.getAllAccountSummaries(studyId), AccountSummary::getId);
        }

        int numUsers = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (userIdIterator.hasNext()) {
            // Rate limit
            perUserRateLimiter.acquire();

            // Process
            try {
                String userId = userIdIterator.next();

                try {
                    processAccountForDate(studyId, date, userId);
                } catch (Exception ex) {
                    LOG.error("Error processing user ID " + userId + ": " + ex.getMessage(), ex);
                }
            } catch (Exception ex) {
                LOG.error("Error getting next user: " + ex.getMessage(), ex);
            }

            // Reporting
            numUsers++;
            if (numUsers % REPORTING_INTERVAL == 0) {
                LOG.info("Processing users in progress: " + numUsers + " users in " +
                        stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            }
        }

        // Write to Worker Log in DDB so we can signal end of processing.
        dynamoHelper.writeWorkerLog(tag);

        LOG.info("Finished processing users: " + numUsers + " users in " + stopwatch.elapsed(TimeUnit.SECONDS) +
                " seconds");
        LOG.info("Finished processing request for study " + studyId + " and date " + dateString);
    }

    // Processes a single user for the given date. Package-scoped for unit tests.
    void processAccountForDate(String studyId, LocalDate date, String userId) throws IOException {
        // Get participant. We'll need some attributes.
        StudyParticipant participant = bridgeHelper.getParticipant(studyId, userId);

        // Exclude users who are not eligible for notifications.
        if (shouldExcludeUser(studyId, participant)) {
            return;
        }

        // Get user's activity events. Filter events that aren't study burst starts.
        WorkerConfig workerConfig = dynamoHelper.getNotificationConfigForStudy(studyId);
        List<ActivityEvent> activityEventList = bridgeHelper.getActivityEvents(studyId, participant.getId());
        List<ActivityEvent> filteredActivityEventList = activityEventList.stream()
                .filter(activityEvent -> workerConfig.getBurstStartEventIdSet().contains(activityEvent.getEventId()))
                .collect(Collectors.toList());

        // Find the upcoming burst, if a burst is coming up tomorrow.
        ActivityEvent upcomingBurstEvent = findUpcomingActivityBurstEvent(date, participant,
                filteredActivityEventList);
        if (upcomingBurstEvent != null) {
            // Notify user of upcoming burst.
            notifyUser(studyId, participant, NotificationType.PRE_BURST);

            // If the burst starts tomorrow, no need to check if the user is in the middle of a burst.
            return;
        }

        // Find the current activity burst.
        ActivityEvent burstEvent = findCurrentActivityBurstEventForParticipant(studyId, date, participant,
                filteredActivityEventList);
        if (burstEvent == null) {
             // We're not currently in an activity burst. (Or we are, but we're in the blackout period.) Skip
            // processing this user.
            return;
        }

        // Determine if we need to notify the user.
        NotificationType notificationType = getNotificationTypeForUser(studyId, date, participant, burstEvent);
        if (notificationType != null) {
            notifyUser(studyId, participant, notificationType);
        }
    }

    // Helper method to determine if a user is ineligible for receiving notifications.
    private boolean shouldExcludeUser(String studyId, StudyParticipant participant) {
        WorkerConfig workerConfig = dynamoHelper.getNotificationConfigForStudy(studyId);
        Set<String> excludedDataGroupSet = workerConfig.getExcludedDataGroupSet();

        // Unverified phone numbers can't be notified
        if (Boolean.FALSE.equals(participant.getPhoneVerified())) {
            return true;
        }

        // Users without timezones can't be processed
        if (participant.getTimeZone() == null) {
            return true;
        }

        // Users with timezone < UTC-11 or > UTC-1 should be excluded. This is because we'd end up sending at unusually
        // early or unusually late hours.
        DateTimeZone timeZone = DateUtils.parseZoneFromOffsetString(participant.getTimeZone());
        int timeZoneOffsetMillis = timeZone.getOffset(DateTime.now());
        if (timeZoneOffsetMillis < MIN_TIMEZONE_OFFSET_MILLIS || timeZoneOffsetMillis > MAX_TIMEZONE_OFFSET_MILLIS) {
            return true;
        }

        // Unconsented users can't be notified
        if (!isUserConsented(studyId, participant)) {
            return true;
        }

        // Check required and excluded data groups.
        // If the user has any of the excluded data groups, exclude the user
        for (String oneUserDataGroup : participant.getDataGroups()) {
            if (excludedDataGroupSet.contains(oneUserDataGroup)) {
                return true;
            }
        }

        // If user was already sent a notification in the last burst duration, don't send another one
        // Special case: If that notification was a PRE_BURST notification, that's fine.
        UserNotification lastNotification = dynamoHelper.getLastNotificationTimeForUser(participant.getId());
        //noinspection RedundantIfStatement
        if (lastNotification != null &&
                lastNotification.getTime() > DateTime.now().minusDays(workerConfig.getBurstDurationDays()).getMillis() &&
                lastNotification.getType() != NotificationType.PRE_BURST) {
            return true;
        }

        // We've checked all the exclude conditions. Do not exclude user.
        return false;
    }

    // Helper method to determine if the user has signed the required consent(s).
    private boolean isUserConsented(String studyId, StudyParticipant participant) {
        // Check each required subpop. The user must have signed that consent.
        WorkerConfig workerConfig = dynamoHelper.getNotificationConfigForStudy(studyId);
        Map<String, List<UserConsentHistory>> consentsBySubpop = participant.getConsentHistories();
        for (String oneRequiredSubpopGuid : workerConfig.getRequiredSubpopulationGuidSet()) {
            List<UserConsentHistory> oneConsentList = consentsBySubpop.get(oneRequiredSubpopGuid);
            if (oneConsentList.isEmpty()) {
                return false;
            }

            // Newest consent is always at the end. If consent signature exists and is not withdrawn, then the user is
            // consented. This is consistent with the 412 logic in BridgePF.
            UserConsentHistory newestConsent = oneConsentList.get(oneConsentList.size() - 1);
            if (newestConsent.getWithdrewOn() != null) {
                return false;
            }
        }

        // If we make it this far, then we've verified that all consents are signed, up-to-date, and not withdrawn.
        return true;
    }

    // Helper method to determine if there's an upcoming study burst coming up tomorrow.
    private ActivityEvent findUpcomingActivityBurstEvent(LocalDate date, StudyParticipant participant,
            List<ActivityEvent> activityEventList) {
        DateTimeZone timeZone = DateUtils.parseZoneFromOffsetString(participant.getTimeZone());

        for (ActivityEvent oneActivityEvent : activityEventList) {
            if (oneActivityEvent.getTimestamp().withZone(timeZone).toLocalDate().minusDays(1).equals(date)) {
                // If the burst start is tomorrow, then we've found it!
                return oneActivityEvent;
            }
        }

        // If we make it this far, we didn't find any activities starting tomorrow.
        return null;
    }

    // Helper method to determine the study burst event that we should be processing for this user.
    private ActivityEvent findCurrentActivityBurstEventForParticipant(String studyId, LocalDate date,
            StudyParticipant participant, List<ActivityEvent> activityEventList) {
        WorkerConfig workerConfig = dynamoHelper.getNotificationConfigForStudy(studyId);
        for (ActivityEvent oneActivityEvent : activityEventList) {
            // Calculate burst bounds. End date is start + period - 1. Skip if the current day is not within the burst
            // period (inclusive).
            DateTimeZone timeZone = DateUtils.parseZoneFromOffsetString(participant.getTimeZone());
            LocalDate burstStartDate = oneActivityEvent.getTimestamp().withZone(timeZone).toLocalDate();
            LocalDate burstEndDate = burstStartDate.plusDays(workerConfig.getBurstDurationDays()).minusDays(1);
            if (date.isBefore(burstStartDate) || date.isAfter(burstEndDate)) {
                continue;
            }

            // We found the current activity burst. Activity bursts do not overlap, so we don't need to look at any
            // other activity events. However, we want to check the notification blackout periods. If we're still
            // within the blackout period, return null.
            LocalDate notificationStartDate = burstStartDate.plusDays(workerConfig
                    .getNotificationBlackoutDaysFromStart());
            LocalDate notificationEndDate = burstEndDate.minusDays(workerConfig.getNotificationBlackoutDaysFromEnd());
            if (date.isBefore(notificationStartDate) || date.isAfter(notificationEndDate)) {
                return null;
            }

            // We should process based on this activity event.
            return oneActivityEvent;
        }

        // We checked all the activity events, and we've determined we're not currently in an activity burst.
        return null;
    }

    // Helper method which looks at the participant's activities to determine if we should send a notification.
    // Returns the notification type (or null if we shouldn't send a notification).
    private NotificationType getNotificationTypeForUser(String studyId, LocalDate date, StudyParticipant participant,
            ActivityEvent burstEvent) {
        String userId = participant.getId();

        WorkerConfig workerConfig = dynamoHelper.getNotificationConfigForStudy(studyId);
        String taskId = workerConfig.getBurstTaskId();
        int numMissedDaysToNotify = workerConfig.getNumMissedDaysToNotify();
        int numMissedConsecutiveDaysToNotify = workerConfig.getNumMissedConsecutiveDaysToNotify();

        // Get user's activities between the burst start and now, including today's activities. Note that because of
        // how scheduling works, we might have tasks scheduled on midnight before the start of the activity burst.
        DateTimeZone timeZone = DateUtils.parseZoneFromOffsetString(participant.getTimeZone());
        LocalDate burstStartDate = burstEvent.getTimestamp().withZone(timeZone).toLocalDate();
        DateTime activityRangeStart = burstStartDate.toDateTimeAtStartOfDay(timeZone);
        DateTime activityRangeEnd = date.plusDays(1).toDateTimeAtStartOfDay(timeZone);
        Iterator<ScheduledActivity> activityIterator = bridgeHelper.getTaskHistory(studyId, userId, taskId,
                activityRangeStart, activityRangeEnd);

        // If the user somehow has no activities with this task ID, don't notify the user. The account is probably not
        // fully bootstrapped, and we should avoid sending them a notification.
        if (!activityIterator.hasNext()) {
            return null;
        }

        // Map the events by scheduled date so it's easier to work with.
        Map<LocalDate, ScheduledActivity> activitiesByDate = new HashMap<>();
        while (activityIterator.hasNext()) {
            ScheduledActivity oneActivity = activityIterator.next();
            LocalDate scheduleDate = oneActivity.getScheduledOn().withZone(timeZone).toLocalDate();
            if (activitiesByDate.containsKey(scheduleDate)) {
                // This shouldn't happen. If it does, log a warning and move on.
                LOG.warn("Duplicate activities found for userId=" + userId + ", taskId=" + taskId + ", date=" +
                        scheduleDate);
            } else {
                activitiesByDate.put(scheduleDate, oneActivity);
            }
        }

        // Check today's activities first. If they did today's activities, don't bother notifying.
        ScheduledActivity todaysActivity = activitiesByDate.get(date);
        if (todaysActivity != null && todaysActivity.getStatus() == ScheduleStatus.FINISHED) {
            return null;
        }

        // Loop through activities in order by date
        int daysMissed = 0;
        int consecutiveDaysMissed = 0;
        int numDays = 0;
        int numActivitiesCompleted = 0;
        for (LocalDate d = burstStartDate; !d.isAfter(date); d = d.plusDays(1)) {
            ScheduledActivity daysActivity = activitiesByDate.get(d);
            if (daysActivity == null || daysActivity.getStatus() != ScheduleStatus.FINISHED) {
                daysMissed++;
                consecutiveDaysMissed++;

                if (daysMissed >= numMissedDaysToNotify) {
                    return NotificationType.CUMULATIVE;
                }
                if (consecutiveDaysMissed >= numMissedConsecutiveDaysToNotify) {
                    if (numDays < workerConfig.getEarlyLateCutoffDays()) {
                        return NotificationType.EARLY;
                    } else {
                        return NotificationType.LATE;
                    }
                }
            } else {
                consecutiveDaysMissed = 0;
                numActivitiesCompleted++;

                if (numActivitiesCompleted >= workerConfig.getNumActivitiesToCompleteBurst()) {
                    // Participant has completed requisite number of activities to complete the study burst. We won't
                    // send a notification. No need to process any further.
                    return null;
                }
            }

            // Increment the day counter so we can determine early vs late.
            numDays++;
        }

        // If we make it this far, we've determined we don't need to notify.
        return null;
    }

    // Encapsulates sending an SMS notification to the user.
    private void notifyUser(String studyId, StudyParticipant participant, NotificationType notificationType)
            throws IOException {
        String userId = participant.getId();
        WorkerConfig workerConfig = dynamoHelper.getNotificationConfigForStudy(studyId);

        // Get notification messages for type.
        List<String> messageList = null;
        switch (notificationType) {
            case CUMULATIVE:
                messageList = workerConfig.getMissedCumulativeActivitiesMessagesList();
                break;
            case EARLY:
                messageList = workerConfig.getMissedEarlyActivitiesMessagesList();
                break;
            case LATE:
                messageList = workerConfig.getMissedLaterActivitiesMessagesList();
                break;
            case PRE_BURST:
                // Narrow down notification messages for data group.
                Map<String, List<String>> messagesByDataGroup = workerConfig.getPreburstMessagesByDataGroup();
                for (String oneDataGroup : participant.getDataGroups()) {
                    if (messagesByDataGroup.containsKey(oneDataGroup)) {
                        messageList = messagesByDataGroup.get(oneDataGroup);
                        break;
                    }
                }
                break;
            default:
                throw new IllegalStateException("Unexpected type " + notificationType);
        }

        if (messageList == null) {
            throw new IllegalStateException("No messages found for type " + notificationType + " for user " + userId);
        }

        // Pick message at random.
        int randomIndex = rng.nextInt(messageList.size());
        String message = messageList.get(randomIndex);

        // Resolve template variables.
        message = templateVariableHelper.resolveTemplateVariables(studyId, participant, message);

        LOG.info("Sending " + notificationType.name() + " notification to user " + userId);

        // Log in Dynamo that we notified this user
        UserNotification userNotification = new UserNotification();
        userNotification.setMessage(message);
        userNotification.setTime(DateUtils.getCurrentMillisFromEpoch());
        userNotification.setType(notificationType);
        userNotification.setUserId(userId);
        dynamoHelper.setLastNotificationTimeForUser(userNotification);

        // Send SMS
        bridgeHelper.sendSmsToUser(studyId, userId, message);
    }
}
