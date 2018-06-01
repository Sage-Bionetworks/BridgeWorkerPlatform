package org.sagebionetworks.bridge.fitbit.schema;

/** Describes the type of parameters used to fill in a URL used to call a FitBit endpoint. */
public enum UrlParameterType {
    /** The job run date, in YYYY-MM-DD. */
    DATE,

    /** The FitBit user ID. */
    USER_ID
}
