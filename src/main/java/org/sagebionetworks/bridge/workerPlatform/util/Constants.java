package org.sagebionetworks.bridge.workerPlatform.util;

/** Constants used by the Worker */
public class Constants {
    public static final String SERVICE_TYPE_REPORTER = "REPORTER";
    public static final String SERVICE_TYPE_UDD = "UDD";
    public static final String SERVICE_TYPE_PARTICIPANT_ROSTER_DOWNLOADER = "PARTICIPANT_ROSTER_DOWNLOADER";

    // Synapse column names
    public static final String COLUMN_HEALTH_CODE = "healthCode";
    public static final String COLUMN_CREATED_DATE = "createdDate";
    public static final String COLUMN_RAW_DATA = "rawData";

    // FitBit vendor ID, as configured in Bridge Server
    public static final String FITBIT_VENDOR_ID = "fitbit";
}
