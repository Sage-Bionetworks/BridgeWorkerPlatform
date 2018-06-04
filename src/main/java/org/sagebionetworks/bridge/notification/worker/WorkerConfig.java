package org.sagebionetworks.bridge.notification.worker;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/** Encapsulates configuration values necessary to determine if and when to send notifications. */
public class WorkerConfig {
    private int burstDurationDays;
    private Set<String> burstStartEventIdSet = ImmutableSet.of();
    private String burstTaskId;
    private int earlyLateCutoffDays;
    private Set<String> excludedDataGroupSet = ImmutableSet.of();
    private Map<String, String> missedCumulativeActivitiesMessagesByDataGroup = ImmutableMap.of();
    private Map<String, String> missedEarlyActivitiesMessagesByDataGroup = ImmutableMap.of();
    private Map<String, String> missedLaterActivitiesMessagesByDataGroup = ImmutableMap.of();
    private int notificationBlackoutDaysFromStart;
    private int notificationBlackoutDaysFromEnd;
    private int numActivitiesToCompleteBurst;
    private int numMissedConsecutiveDaysToNotify;
    private int numMissedDaysToNotify;
    private Map<String, String> preburstMessagesByDataGroup = ImmutableMap.of();
    private Set<String> requiredDataGroupsOneOfSet = ImmutableSet.of();
    private Set<String> requiredSubpopulationGuidSet = ImmutableSet.of();

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
     * Messages to send if the participant misses a cumulative total of activities during this study burst, keyed by
     * data group. The keys in this set should match the keys in {@link #getRequiredDataGroupsOneOfSet}.
     */
    public Map<String, String> getMissedCumulativeActivitiesMessagesByDataGroup() {
        return missedCumulativeActivitiesMessagesByDataGroup;
    }

    /** @see #getMissedCumulativeActivitiesMessagesByDataGroup */
    public void setMissedCumulativeActivitiesMessagesByDataGroup(
            Map<String, String> missedCumulativeActivitiesMessagesByDataGroup) {
        this.missedCumulativeActivitiesMessagesByDataGroup = missedCumulativeActivitiesMessagesByDataGroup != null ?
                ImmutableMap.copyOf(missedCumulativeActivitiesMessagesByDataGroup) : ImmutableMap.of();
    }

    /**
     * Messages to send if the participant misses consecutive activities early in the study burst, keyed by data group.
     * The keys in this set should match the keys in {@link #getRequiredDataGroupsOneOfSet}.
     */
    public Map<String, String> getMissedEarlyActivitiesMessagesByDataGroup() {
        return missedEarlyActivitiesMessagesByDataGroup;
    }

    /** @see #getMissedEarlyActivitiesMessagesByDataGroup */
    public void setMissedEarlyActivitiesMessagesByDataGroup(
            Map<String, String> missedEarlyActivitiesMessagesByDataGroup) {
        this.missedEarlyActivitiesMessagesByDataGroup = missedEarlyActivitiesMessagesByDataGroup != null ?
                ImmutableMap.copyOf(missedEarlyActivitiesMessagesByDataGroup) : ImmutableMap.of();
    }

    /**
     * Messages to send if the participant misses consecutive activities late in the study burst, keyed by data group.
     * The keys in this set should match the keys in {@link #getRequiredDataGroupsOneOfSet}.
     */
    public Map<String, String> getMissedLaterActivitiesMessagesByDataGroup() {
        return missedLaterActivitiesMessagesByDataGroup;
    }

    /** @see #getMissedLaterActivitiesMessagesByDataGroup */
    public void setMissedLaterActivitiesMessagesByDataGroup(
            Map<String, String> missedLaterActivitiesMessagesByDataGroup) {
        this.missedLaterActivitiesMessagesByDataGroup = missedLaterActivitiesMessagesByDataGroup != null ?
                ImmutableMap.copyOf(missedLaterActivitiesMessagesByDataGroup) : ImmutableMap.of();
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
     * Messages to send before the start of the study burst, keyed by data group. The keys in this set should match the
     * keys in {@link #getRequiredDataGroupsOneOfSet}.
     */
    public Map<String, String> getPreburstMessagesByDataGroup() {
        return preburstMessagesByDataGroup;
    }

    /** @see #getPreburstMessagesByDataGroup */
    public void setPreburstMessagesByDataGroup(Map<String, String> preburstMessagesByDataGroup) {
        this.preburstMessagesByDataGroup = preburstMessagesByDataGroup != null ?
                ImmutableMap.copyOf(preburstMessagesByDataGroup) : ImmutableMap.of();
    }

    /** Participant must be in one of these data groups to receive notifications. */
    public Set<String> getRequiredDataGroupsOneOfSet() {
        return requiredDataGroupsOneOfSet;
    }

    /** @see #getRequiredDataGroupsOneOfSet */
    public void setRequiredDataGroupsOneOfSet(Set<String> requiredDataGroupsOneOfSet) {
        this.requiredDataGroupsOneOfSet = requiredDataGroupsOneOfSet != null ?
                ImmutableSet.copyOf(requiredDataGroupsOneOfSet) : ImmutableSet.of();
    }

    /** Set of subpopulations that the participant must be consented to in order to receive notifications. */
    public Set<String> getRequiredSubpopulationGuidSet() {
        return requiredSubpopulationGuidSet;
    }

    /** @see #getRequiredSubpopulationGuidSet */
    public void setRequiredSubpopulationGuidSet(Set<String> requiredSubpopulationGuidSet) {
        this.requiredSubpopulationGuidSet = requiredSubpopulationGuidSet != null ?
                ImmutableSet.copyOf(requiredSubpopulationGuidSet) : ImmutableSet.of();
    }
}
