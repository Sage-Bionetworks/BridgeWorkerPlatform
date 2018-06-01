package org.sagebionetworks.bridge.fitbit.worker;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.client.fluent.Request;
import org.joda.time.DateTime;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.fitbit.bridge.FitBitUser;
import org.sagebionetworks.bridge.fitbit.schema.ColumnSchema;
import org.sagebionetworks.bridge.fitbit.schema.EndpointSchema;
import org.sagebionetworks.bridge.fitbit.schema.TableSchema;
import org.sagebionetworks.bridge.fitbit.schema.UrlParameterType;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.synapse.SynapseHelper;

/** The User Processor downloads data from the FitBit Web API and collates the data into tables. */
@Component
public class UserProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(UserProcessor.class);

    private FileHelper fileHelper;
    private SynapseHelper synapseHelper;

    /** File Helper, used to write files to the temp directory before uploading as file handles. */
    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    /** Synapse Helper, used to upload files as file handles to Synapse. */
    @Autowired
    public final void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    /** Processes the given endpoint for the given user. This is the main entry point into the User Processor. */
    public void processEndpointForUser(RequestContext ctx, FitBitUser user, EndpointSchema endpointSchema)
            throws IOException, SynapseException {
        // Generate url parameters
        List<String> resolvedUrlParamList = new ArrayList<>();
        for (UrlParameterType oneUrlParam : endpointSchema.getUrlParameters()) {
            switch (oneUrlParam) {
                case DATE:
                    resolvedUrlParamList.add(ctx.getDate());
                    break;
                case USER_ID:
                    resolvedUrlParamList.add(user.getUserId());
                    break;
            }
        }

        // Get data from FitBit
        String url = String.format(endpointSchema.getUrl(), resolvedUrlParamList.toArray());
        String response = makeHttpRequest(url, user.getAccessToken());
        JsonNode responseNode = DefaultObjectMapper.INSTANCE.readTree(response);

        // Process each key (top-level table) in the response
        Iterator<String> responseKeyIter = responseNode.fieldNames();
        while (responseKeyIter.hasNext()) {
            String oneResponseKey = responseKeyIter.next();
            String tableId = endpointSchema.getEndpointId() + '.' + oneResponseKey;
            JsonNode dataNode = responseNode.get(oneResponseKey);

            TableSchema oneTableSchema = endpointSchema.getTablesByKey().get(oneResponseKey);
            if (oneTableSchema != null) {
                ctx.getPopulatedTablesById().computeIfAbsent(tableId, key -> new PopulatedTable(tableId,
                        oneTableSchema));

                if (dataNode.isArray()) {
                    // dataNode is a list of rows
                    for (JsonNode rowNode : dataNode) {
                        processTableRowForUser(ctx, user, endpointSchema, oneTableSchema, rowNode);
                    }
                } else if (dataNode.isObject()) {
                    // The object is the row we need to process.
                    processTableRowForUser(ctx, user, endpointSchema, oneTableSchema, dataNode);
                } else {
                    warnWrapper("Table " + tableId + " is neither array nor object for user " +
                            user.getHealthCode());
                }
            } else if (!endpointSchema.getIgnoredKeys().contains(oneResponseKey)) {
                warnWrapper("Unexpected table " + tableId + " for user " + user.getHealthCode());
            }
        }
    }

    // Helper to process a single row of FitBit data.
    private void processTableRowForUser(RequestContext ctx, FitBitUser user, EndpointSchema endpointSchema,
            TableSchema tableSchema, JsonNode rowNode) throws IOException, SynapseException {
        String tableId = endpointSchema.getEndpointId() + '.' + tableSchema.getTableKey();
        PopulatedTable populatedTable = ctx.getPopulatedTablesById().get(tableId);
        Map<String, String> rowValueMap = new HashMap<>();

        // Iterate through all values in the node. Serialize the values into the PopulatedTable.
        Iterator<String> columnNameIter = rowNode.fieldNames();
        while (columnNameIter.hasNext()) {
            String oneColumnName = columnNameIter.next();
            JsonNode columnValueNode = rowNode.get(oneColumnName);

            ColumnSchema columnSchema = tableSchema.getColumnsById().get(oneColumnName);
            if (columnSchema == null) {
                warnWrapper("Unexpected column " + oneColumnName + " in table " + tableId + " for user " +
                        user.getHealthCode());
            } else {
                Object value = serializeJsonForColumn(ctx, columnValueNode, columnSchema);
                if (value != null) {
                    rowValueMap.put(oneColumnName, value.toString());
                }
            }
        }

        if (!rowValueMap.isEmpty()) {
            // Always include the user's health code and the created date.
            rowValueMap.put(Constants.COLUMN_HEALTH_CODE, user.getHealthCode());
            rowValueMap.put(Constants.COLUMN_CREATED_DATE, ctx.getDate());

            // Add the row to the table
            populatedTable.getRowList().add(rowValueMap);
        }
    }

    // Helper method to serialize a JsonNode to write to the given Column.
    // Visible for testing.
    String serializeJsonForColumn(RequestContext ctx, JsonNode node, ColumnSchema columnSchema)
            throws IOException, SynapseException {
        String columnId = columnSchema.getColumnId();

        // Short-cut: null check.
        if (node == null || node.isNull()) {
            return null;
        }

        // Canonicalize into an object.
        Object value = null;
        switch (columnSchema.getColumnType()) {
            case BOOLEAN:
                if (node.isBoolean()) {
                    value = node.booleanValue();
                } else {
                    LOG.warn("Expected boolean for column " + columnId + ", got " + node.getNodeType().name());
                }
                break;
            case DATE:
                // Currently, all dates from FitBit web API are in UTC, so we can just use epoch milliseconds,
                // which is what Synapse expects anyway.
                if (node.isTextual()) {
                    String dateTimeStr = node.textValue();
                    try {
                        value = DateTime.parse(dateTimeStr).getMillis();
                    } catch (IllegalArgumentException ex) {
                        LOG.warn("Invalid DateTime format " + dateTimeStr);
                    }
                } else {
                    LOG.warn("Expected string for column " + columnId + ", got " + node.getNodeType().name());
                }
                break;
            case DOUBLE:
                if (node.isNumber()) {
                    value = node.decimalValue().toPlainString();
                } else {
                    LOG.warn("Expected number for column " + columnId + ", got " + node.getNodeType().name());
                }
                break;
            case FILEHANDLEID:
                // Write value to temp file on disk.
                String tempFileName = columnId + RandomStringUtils.randomAlphabetic(4);
                File fileToUpload = fileHelper.newFile(ctx.getTmpDir(), tempFileName);
                try (OutputStream fileOutputStream = fileHelper.getOutputStream(fileToUpload)) {
                    DefaultObjectMapper.INSTANCE.writeValue(fileOutputStream, node);
                }

                // Upload file handle. Value is the file handle ID.
                FileHandle fileHandle = synapseHelper.createFileHandleWithRetry(fileToUpload);
                value = fileHandle.getId();

                // Finally, delete the temp file.
                fileHelper.deleteFile(fileToUpload);
                break;
            case INTEGER:
                if (node.isNumber()) {
                    value = node.longValue();
                } else {
                    LOG.warn("Expected number for column " + columnId + ", got " + node.getNodeType().name());
                }
                break;
            case LARGETEXT:
                // LargeText is used for when the value is an array or an object. In this case, we want to
                // write the JSON verbatim to Synapse.
                value = node;
                break;
            case STRING:
                String textValue;
                if (node.isTextual()) {
                    textValue = node.textValue();
                } else {
                    textValue = node.toString();
                }

                // Strings have a max length. If the string is too long, truncate it.
                int valueLength = textValue.length();
                int maxLength = columnSchema.getMaxLength();
                if (valueLength > maxLength) {
                    LOG.warn("Truncating value of length " + valueLength + " to max length " + maxLength +
                            " for column " + columnId);
                    textValue = textValue.substring(0, maxLength);
                }
                value = textValue;
                break;
            default:
                LOG.warn("Unexpected type " + columnSchema.getColumnType().name() + " for column " + columnId);
                break;
        }

        // If the canonicalized value is null (possibly because of type errors), return null instead of converting to
        // a string.
        if (value == null) {
            return null;
        }

        // Convert to string.
        return String.valueOf(value);
    }

    // Abstracts away the HTTP call to FitBit Web API.
    // Visible for testing.
    String makeHttpRequest(String url, String accessToken) throws IOException {
        return Request.Get(url).setHeader("Authorization", "Bearer " + accessToken).execute()
                .returnContent().asString();
    }

    // Warn wrapper, so that we can use mocks and spies to verify that we're handling unusual cases.
    // Visible for testing
    void warnWrapper(String msg) {
        LOG.warn(msg);
    }
}
