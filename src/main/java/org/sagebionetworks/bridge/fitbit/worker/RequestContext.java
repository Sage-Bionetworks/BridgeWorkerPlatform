package org.sagebionetworks.bridge.fitbit.worker;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.sagebionetworks.bridge.rest.model.App;

/** Represents the context needed to run a FitBit export for the given date and app. */
public class RequestContext {
    // Instance invariants
    private final String date;
    private final App app;
    private final File tmpDir;

    // Instance state tracking
    private final Map<String, PopulatedTable> populatedTablesById = new HashMap<>();

    /**
     * Constructs a Request Context
     * @param date request date
     * @param app request app
     * @param tmpDir temp directory
     */
    public RequestContext(String date, App app, File tmpDir) {
        this.date = date;
        this.app = app;
        this.tmpDir = tmpDir;
    }

    /** Date that the worker should download data for, in YYYY-MM-DD format. */
    public String getDate() {
        return date;
    }

    /** App that the worker should download data for. */
    public App getApp() {
        return app;
    }

    /** Temporary directory on disk that the worker can use as scratch space. */
    public File getTmpDir() {
        return tmpDir;
    }

    /**
     * Map of populated tables. The key is the table ID, which is unique per app. The values are populated tables,
     * which are used to tabulate data to be exported to Synapse.
     */
    public Map<String, PopulatedTable> getPopulatedTablesById() {
        return populatedTablesById;
    }
}
