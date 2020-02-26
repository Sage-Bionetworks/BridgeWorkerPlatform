package org.sagebionetworks.bridge.notification.worker;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/** Encapsulates configuration values necessary to determine if and when to send notifications. */
public class WorkerConfig {
    private String appUrl;
    private int burstDurationDays;
    private Set<String> burstStartEventIdSet = ImmutableSet.of();
    private String burstTaskId;
    private String defaultPreburstMessage;
    private int earlyLateCutoffDays;
    private String engagementSurveyGuid;
    private Set<String> excludedDataGroupSet = ImmutableSet.of();
    private List<String> missedCumulativeActivitiesMessagesList = ImmutableList.of();
    private List<String> missedEarlyActivitiesMessagesList = ImmutableList.of();
    private List<String> missedLaterActivitiesMessagesList = ImmutableList.of();
    private int notificationBlackoutDaysFromStart;
    private int notificationBlackoutDaysFromEnd;
    private int numActivitiesToCompleteBurst;
    private int numMissedConsecutiveDaysToNotify;
    private int numMissedDaysToNotify;
    private Map<String, List<String>> preburstMessagesByDataGroup = ImmutableMap.of();

    /**
     * URL that links back to the app (or to the study website, if for some reason the app is not installed).
     * Example: http://mpower.sagebridge.org/
     */
    public String getAppUrl() {
        return appUrl;
    }

    /** @see #getAppUrl */
    public void setAppUrl(String appUrl) {
        this.appUrl = appUrl;
    }

    /** The length of the study burst, in days. */
    public int getBurstDurationDays() {
        return burstDurationDays;
    }

    /** @see #getBurstDurationDays */
    public void setBurstDurationDays(int burstDurationDays) {
        this.burstDurationDays = burstDurationDays;
    }

    /** The set of activity event IDs that mark the start of a study burst. May be empty, but never null/ */
    public Set<String> getBurstStartEventIdSet() {
        return burstStartEventIdSet;
    }

    /** @see #getBurstStartEventIdSet */
    public void setBurstStartEventIdSet(Set<String> burstStartEventIdSet) {
        this.burstStartEventIdSet = burstStartEventIdSet != null ? ImmutableSet.copyOf(burstStartEventIdSet) :
                ImmutableSet.of();
    }

    /** Task ID that the participant is expected to complete or receive notifications. */
    public String getBurstTaskId() {
        return burstTaskId;
    }

    /** @see #getBurstTaskId */
    public void setBurstTaskId(String burstTaskId) {
        this.burstTaskId = burstTaskId;
    }

    /** The default pre-burst notification if the participant lacks the data groups or has no study commitment. */
    public String getDefaultPreburstMessage() {
        return defaultPreburstMessage;
    }

    /** @see #getDefaultPreburstMessage */
    public void setDefaultPreburstMessage(String defaultPreburstMessage) {
        this.defaultPreburstMessage = defaultPreburstMessage;
    }

    /**
     * Number of days that have passed in the burst before we consider it "late burst". This doesn't include today's
     * date. If this is set to 0, all days are considered "late burst". If this is set to 1, the first day is
     * considered "early", and the rest are considered "late". And so forth.
     */
    public int getEarlyLateCutoffDays() {
        return earlyLateCutoffDays;
    }

    /** @see #getEarlyLateCutoffDays */
    public void setEarlyLateCutoffDays(int earlyLateCutoffDays) {
        this.earlyLateCutoffDays = earlyLateCutoffDays;
    }

    /** Survey GUID for the engagement survey, for use with the "studyCommitment" template variable. */
    public String getEngagementSurveyGuid() {
        return engagementSurveyGuid;
    }

    /** @see #getEngagementSurveyGuid */
    public void setEngagementSurveyGuid(String engagementSurveyGuid) {
        this.engagementSurveyGuid = engagementSurveyGuid;
    }

    /** Set of data groups that will never receive notifications. */
    public Set<String> getExcludedDataGroupSet() {
        return excludedDataGroupSet;
    }

    /** @see #getExcludedDataGroupSet */
    public void setExcludedDataGroupSet(Set<String> excludedDataGroupSet) {
        this.excludedDataGroupSet = excludedDataGroupSet != null ? ImmutableSet.copyOf(excludedDataGroupSet) :
                ImmutableSet.of();
    }

    /**
     * Messages to send if the participant misses a cumulative total of activities during this study burst. Worker
     * will pick one at random.
     */
    public List<String> getMissedCumulativeActivitiesMessagesList() {
        return missedCumulativeActivitiesMessagesList;
    }

    /** @see #getMissedCumulativeActivitiesMessagesList */
    public void setMissedCumulativeActivitiesMessagesList(List<String> missedCumulativeActivitiesMessagesList) {
        this.missedCumulativeActivitiesMessagesList = missedCumulativeActivitiesMessagesList != null ?
                ImmutableList.copyOf(missedCumulativeActivitiesMessagesList) : ImmutableList.of();
    }

    /**
     * Messages to send if the participant misses consecutive activities early in the study burst. Worker will pick one
     * at random.
     */
    public List<String> getMissedEarlyActivitiesMessagesList() {
        return missedEarlyActivitiesMessagesList;
    }

    /** @see #getMissedEarlyActivitiesMessagesList */
    public void setMissedEarlyActivitiesMessagesList(List<String> missedEarlyActivitiesMessagesList) {
        this.missedEarlyActivitiesMessagesList = missedEarlyActivitiesMessagesList != null ?
                ImmutableList.copyOf(missedEarlyActivitiesMessagesList) : ImmutableList.of();
    }

    /**
     * Messages to send if the participant misses consecutive activities late in the study burst. Worker will pick one
     * at random.
     */
    public List<String> getMissedLaterActivitiesMessagesList() {
        return missedLaterActivitiesMessagesList;
    }

    /** @see #getMissedLaterActivitiesMessagesList */
    public void setMissedLaterActivitiesMessagesList(List<String> missedLaterActivitiesMessagesList) {
        this.missedLaterActivitiesMessagesList = missedLaterActivitiesMessagesList != null ?
                ImmutableList.copyOf(missedLaterActivitiesMessagesList) : ImmutableList.of();
    }

    /** Number of days at the start of the study burst where we don't send notifications. */
    public int getNotificationBlackoutDaysFromStart() {
        return notificationBlackoutDaysFromStart;
    }

    /** @see #getNotificationBlackoutDaysFromStart */
    public void setNotificationBlackoutDaysFromStart(int notificationBlackoutDaysFromStart) {
        this.notificationBlackoutDaysFromStart = notificationBlackoutDaysFromStart;
    }

    /** Number of days at the end of the study burst where we don't send notifications. */
    public int getNotificationBlackoutDaysFromEnd() {
        return notificationBlackoutDaysFromEnd;
    }

    /** @see #getNotificationBlackoutDaysFromEnd */
    public void setNotificationBlackoutDaysFromEnd(int notificationBlackoutDaysFromEnd) {
        this.notificationBlackoutDaysFromEnd = notificationBlackoutDaysFromEnd;
    }

    /** The total number of activities the participant needs to complete the study burst. */
    public int getNumActivitiesToCompleteBurst() {
        return numActivitiesToCompleteBurst;
    }

    /** @see #getNumActivitiesToCompleteBurst */
    public void setNumActivitiesToCompleteBurst(int numActivitiesToCompleteBurst) {
        this.numActivitiesToCompleteBurst = numActivitiesToCompleteBurst;
    }

    /** Number of consecutive days of missed activities before we send a notification. */
    public int getNumMissedConsecutiveDaysToNotify() {
        return numMissedConsecutiveDaysToNotify;
    }

    /** @see #getNumMissedConsecutiveDaysToNotify */
    public void setNumMissedConsecutiveDaysToNotify(int numMissedConsecutiveDaysToNotify) {
        this.numMissedConsecutiveDaysToNotify = numMissedConsecutiveDaysToNotify;
    }

    /** Number of cumulative days of missed activities within a single study burst before we send a notification. */
    public int getNumMissedDaysToNotify() {
        return numMissedDaysToNotify;
    }

    /** @see #getNumMissedDaysToNotify */
    public void setNumMissedDaysToNotify(int numMissedDaysToNotify) {
        this.numMissedDaysToNotify = numMissedDaysToNotify;
    }

    /**
     * Messages to send before the start of the study burst, keyed by data group. Each entry is a list of possible
     * messages, and the worker picks one of those messages at random.
     */
    public Map<String, List<String>> getPreburstMessagesByDataGroup() {
        return preburstMessagesByDataGroup;
    }

    /** @see #getPreburstMessagesByDataGroup */
    public void setPreburstMessagesByDataGroup(Map<String, List<String>> preburstMessagesByDataGroup) {
        this.preburstMessagesByDataGroup = preburstMessagesByDataGroup != null ?
                ImmutableMap.copyOf(preburstMessagesByDataGroup) : ImmutableMap.of();
    }
}
