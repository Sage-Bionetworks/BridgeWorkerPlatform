package org.sagebionetworks.bridge.notification.worker;

/** This class encapsulates all the fields necessary to represent a notification sent to a user. */
public class UserNotification {
    private String message;
    private long time;
    private NotificationType type;
    private String userId;

    /** Message sent to the user. */
    public String getMessage() {
        return message;
    }

    /** @see #getMessage */
    public void setMessage(String message) {
        this.message = message;
    }

    /** Time in epoch milliseconds that the message was sent. */
    public long getTime() {
        return time;
    }

    /** @see #getTime */
    public void setTime(long time) {
        this.time = time;
    }

    /** Type of notification sent. */
    public NotificationType getType() {
        return type;
    }

    /** @see #getType */
    public void setType(NotificationType type) {
        this.type = type;
    }

    /** ID of the user the notification was sent to. */
    public String getUserId() {
        return userId;
    }

    /** @see #getUserId */
    public void setUserId(String userId) {
        this.userId = userId;
    }
}
