package org.sagebionetworks.bridge.notification.config;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.sagebionetworks.bridge.dynamodb.DynamoNamingHelper;

@Configuration("notificationConfig")
public class SpringConfig {
    @Bean(name = "ddbNotificationConfigTable")
    @Autowired
    public Table ddbNotificationConfigTable(DynamoDB ddbClient, DynamoNamingHelper namingHelper) {
        String fullyQualifiedTableName = namingHelper.getFullyQualifiedTableName("NotificationConfig");
        return ddbClient.getTable(fullyQualifiedTableName);
    }

    @Bean(name = "ddbNotificationLogTable")
    @Autowired
    public Table ddbNotificationLogTable(DynamoDB ddbClient, DynamoNamingHelper namingHelper) {
        String fullyQualifiedTableName = namingHelper.getFullyQualifiedTableName("NotificationLog");
        return ddbClient.getTable(fullyQualifiedTableName);
    }
}
