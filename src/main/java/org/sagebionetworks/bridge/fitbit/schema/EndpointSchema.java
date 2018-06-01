package org.sagebionetworks.bridge.fitbit.schema;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

/** Metadata for how to call a FitBit Web API endpoint and how to interpret the results. */
@JsonDeserialize(builder = EndpointSchema.Builder.class)
public class EndpointSchema {
    private final String endpointId;
    private final Set<String> ignoredKeys;
    private final String url;
    private final List<UrlParameterType> urlParameters;
    private final List<TableSchema> tables;
    private transient final Map<String, TableSchema> tablesByKey;

    /** Private constructor. To construct, use Builder. */
    private EndpointSchema(String endpointId, Set<String> ignoredKeys, String url,
            List<UrlParameterType> urlParameters, List<TableSchema> tables) {
        this.endpointId = endpointId;
        this.ignoredKeys = ignoredKeys;
        this.url = url;
        this.urlParameters = urlParameters;
        this.tables = tables;
        this.tablesByKey = Maps.uniqueIndex(tables, TableSchema::getTableKey);
    }

    /** Endpoint ID. Uniquely identifies the endpoint in Bridge. Used to derive the Synapse table name. */
    public String getEndpointId() {
        return endpointId;
    }

    /** Top-level keys that should be ignored when importing into Synapse. */
    public Set<String> getIgnoredKeys() {
        return ignoredKeys;
    }

    /** Endpoint URL. URL can have placeholders using %s. */
    public String getUrl() {
        return url;
    }

    /** URL parameter types, used to determine how to fill URL placeholders. */
    public List<UrlParameterType> getUrlParameters() {
        return urlParameters;
    }

    /** List of top-level keys to parse and the columns they represent. */
    public List<TableSchema> getTables() {
        return tables;
    }

    /** Map of tables by table key. */
    @JsonIgnore
    public Map<String, TableSchema> getTablesByKey() {
        return tablesByKey;
    }

    /** Builder */
    public static class Builder {
        private String endpointId;
        private Set<String> ignoredKeys;
        private String url;
        private List<UrlParameterType> urlParameters;
        private List<TableSchema> tables;

        /** @see EndpointSchema#getEndpointId */
        public Builder withEndpointId(String endpointId) {
            this.endpointId = endpointId;
            return this;
        }

        /** @see EndpointSchema#getIgnoredKeys */
        public Builder withIgnoredKeys(Set<String> ignoredKeys) {
            this.ignoredKeys = ignoredKeys;
            return this;
        }

        /** @see EndpointSchema#getUrl */
        public Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        /** @see EndpointSchema#getUrlParameters */
        public Builder withUrlParameters(List<UrlParameterType> urlParameters) {
            this.urlParameters = urlParameters;
            return this;
        }

        /** @see EndpointSchema#getTables */
        public Builder withTables(List<TableSchema> tables) {
            this.tables = tables;
            return this;
        }

        /** Builds an EndpointSchema */
        public EndpointSchema build() {
            // Required params: endpointId, url, tables
            // These must be non-null and non-empty
            if (StringUtils.isBlank(endpointId)) {
                throw new IllegalStateException("endpointId must be specified");
            }
            if (StringUtils.isBlank(url)) {
                throw new IllegalStateException("url must be specified");
            }
            if (tables == null || tables.isEmpty()) {
                throw new IllegalStateException("tables must be non-null and non-empty");
            }

            // Optional params: ignoredKeys, urlParameters
            // If these are null, replace them with empty collections so we don't have to worry about null checks down
            // the line.
            if (ignoredKeys == null) {
                ignoredKeys = ImmutableSet.of();
            }
            if (urlParameters == null) {
                urlParameters = ImmutableList.of();
            }

            return new EndpointSchema(endpointId, ImmutableSet.copyOf(ignoredKeys), url,
                    ImmutableList.copyOf(urlParameters), ImmutableList.copyOf(tables));
        }
    }
}
