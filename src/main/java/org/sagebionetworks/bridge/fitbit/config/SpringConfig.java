package org.sagebionetworks.bridge.fitbit.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.sagebionetworks.bridge.dynamodb.DynamoNamingHelper;
import org.sagebionetworks.bridge.fitbit.schema.EndpointSchema;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;

@Configuration("fitbitConfig")
public class SpringConfig {
    @Bean(name = "endpointSchemas")
    public List<EndpointSchema> endpointSchemas() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream schemaFileStream = classLoader.getResourceAsStream("schema.json");
        List<EndpointSchema> value = DefaultObjectMapper.INSTANCE.readValue(schemaFileStream,
                new TypeReference<List<EndpointSchema>>(){});
        return ImmutableList.copyOf(value);
    }

    @Bean(name = "ddbTablesMap")
    @Autowired
    public Table ddbTablesMap(DynamoDB ddbClient, DynamoNamingHelper namingHelper) {
        String fullyQualifiedTableName = namingHelper.getFullyQualifiedTableName("FitBitTables");
        return ddbClient.getTable(fullyQualifiedTableName);
    }
}
