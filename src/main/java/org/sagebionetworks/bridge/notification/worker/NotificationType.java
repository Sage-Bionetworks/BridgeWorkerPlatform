package org.sagebionetworks.bridge.notification.worker;

/** Represents the type of notification sent to the participant. */
public enum NotificationType {
    /** Sent if the participant misses many cumulative activities in a study burst. */
    CUMULATIVE,

    /** Sent if the participant misses consecutive activities early in the study burst. */
    EARLY,

    /** Sent if the participant misses consecutive activities late in the study burst. */
    LATE,

    /** Notification sent before the start of an activity burst. */
    PRE_BURST,
}
