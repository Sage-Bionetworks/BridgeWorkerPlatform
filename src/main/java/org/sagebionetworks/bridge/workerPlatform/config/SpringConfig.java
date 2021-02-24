package org.sagebionetworks.bridge.workerPlatform.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.fasterxml.jackson.databind.JsonNode;
import org.sagebionetworks.client.SynapseAdminClientImpl;
import org.sagebionetworks.client.SynapseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.config.PropertiesConfig;
import org.sagebionetworks.bridge.dynamodb.DynamoNamingHelper;
import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.heartbeat.HeartbeatLogger;
import org.sagebionetworks.bridge.reporter.worker.BridgeReporterProcessor;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorker;
import org.sagebionetworks.bridge.sqs.SqsHelper;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.udd.worker.BridgeUddProcessor;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.multiplexer.BridgeWorkerPlatformSqsCallback;
import org.sagebionetworks.bridge.workerPlatform.util.Constants;

// These configs get credentials from the default credential chain. For developer desktops, this is ~/.aws/credentials.
// For EC2 instances, this happens transparently.
// See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html and
// http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html#set-up-creds for more info.
@ComponentScan({
        "org.sagebionetworks.bridge.udd",
        "org.sagebionetworks.bridge.uploadredrive",
        "org.sagebionetworks.bridge.workerPlatform",
        "org.sagebionetworks.bridge.participantRoster"
})
@Import({
        org.sagebionetworks.bridge.fitbit.config.SpringConfig.class,
        org.sagebionetworks.bridge.notification.config.SpringConfig.class,
        org.sagebionetworks.bridge.reporter.config.SpringConfig.class,
})
@Configuration("GeneralConfig")
public class SpringConfig {
    private static final Logger LOG = LoggerFactory.getLogger(SpringConfig.class);

    private static final String CONFIG_FILE = "BridgeWorkerPlatform.conf";
    private static final String DEFAULT_CONFIG_FILE = CONFIG_FILE;
    private static final String USER_CONFIG_FILE = System.getProperty("user.home") + "/" + CONFIG_FILE;

    @Bean
    public ClientManager bridgeClientManager() {
        // sign-in credentials
        Config config = bridgeConfig();
        String appId = config.get("bridge.worker.appId");
        String email = config.get("bridge.worker.email");
        String password = config.get("bridge.worker.password");
        SignIn signIn = new SignIn().appId(appId).email(email).password(password);

        ClientInfo clientInfo = new ClientInfo().appName("BridgeWorkerPlatform").appVersion(1);
        return new ClientManager.Builder().withClientInfo(clientInfo).withSignIn(signIn).build();
    }

    @Bean
    public Config bridgeConfig() {
        String defaultConfig = getClass().getClassLoader().getResource(DEFAULT_CONFIG_FILE).getPath();
        Path defaultConfigPath = Paths.get(defaultConfig);
        Path localConfigPath = Paths.get(USER_CONFIG_FILE);

        try {
            if (Files.exists(localConfigPath)) {
                return new PropertiesConfig(defaultConfigPath, localConfigPath);
            } else {
                return new PropertiesConfig(defaultConfigPath);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Bean
    public DynamoDB ddbClient() {
        return new DynamoDB(new AmazonDynamoDBClient());
    }

    @Bean
    public DynamoNamingHelper dynamoNamingHelper() {
        return new DynamoNamingHelper(bridgeConfig());
    }

    @Bean
    public DynamoQueryHelper ddbQueryHelper() {
        return new DynamoQueryHelper();
    }

    @Bean(name = "ddbAppTable")
    public Table ddbAppTable() {
        String fullyQualifiedTableName = dynamoNamingHelper().getFullyQualifiedTableName("Study");
        return ddbClient().getTable(fullyQualifiedTableName);
    }

    @Bean(name = "ddbSynapseMapTable")
    public Table ddbSynapseMapTable() {
        return ddbClient().getTable(bridgeConfig().get("synapse.map.table"));
    }

    @Bean(name = "ddbSynapseMetaTable")
    public Table ddbSynapseMetaTable() {
        return ddbClient().getTable(bridgeConfig().get("synapse.meta.table"));
    }

    // Naming note: This is a DDB table containing references to a set of Synapse tables. The name is a bit confusing,
    // but I'm not sure how to make it less confusing.
    @Bean(name = "ddbSynapseSurveyTablesTable")
    public Table ddbSynapseSurveyTablesTable() {
        String fullyQualifiedTableName = dynamoNamingHelper().getFullyQualifiedTableName(
                "SynapseSurveyTables");
        return ddbClient().getTable(fullyQualifiedTableName);
    }

    @Bean(name = "ddbUploadSchemaTable")
    public Table ddbUploadSchemaTable(Config config) {
        String fullyQualifiedTableName = dynamoNamingHelper().getFullyQualifiedTableName(
                "UploadSchema");
        return ddbClient().getTable(fullyQualifiedTableName);
    }

    @Bean(name = "ddbUploadSchemaAppIndex")
    public Index ddbUploadSchemaAppIndex() {
        return ddbUploadSchemaTable(bridgeConfig()).getIndex("studyId-index");
    }

    @Bean(name = "ddbWorkerLogTable")
    public Table ddbWorkerLogTable() {
        String fullyQualifiedTableName = dynamoNamingHelper().getFullyQualifiedTableName("WorkerLog");
        return ddbClient().getTable(fullyQualifiedTableName);
    }

    @Bean(name = "generalExecutorService")
    public ExecutorService generalExecutorService() {
        return Executors.newFixedThreadPool(bridgeConfig().getInt("threadpool.general.count"));
    }

    @Bean(name = "synapseExecutorService")
    public ExecutorService synapseExecutorService() {
        return Executors.newFixedThreadPool(bridgeConfig().getInt("threadpool.synapse.count"));
    }

    @Bean
    public FileHelper fileHelper() {
        return new FileHelper();
    }

    @Bean
    public HeartbeatLogger heartbeatLogger() {
        HeartbeatLogger heartbeatLogger = new HeartbeatLogger();
        heartbeatLogger.setIntervalMinutes(bridgeConfig().getInt("heartbeat.interval.minutes"));
        return heartbeatLogger;
    }

    @Bean
    public S3Helper s3Helper() {
        S3Helper s3Helper = new S3Helper();
        s3Helper.setS3Client(new AmazonS3Client());
        return s3Helper;
    }

    @Bean
    public AmazonSimpleEmailServiceClient sesClient() {
        return new AmazonSimpleEmailServiceClient();
    }

    @Bean
    public AmazonSNSClient snsClient() {
        return new AmazonSNSClient();
    }

    @Bean
    public SqsHelper sqsHelper() {
        SqsHelper sqsHelper = new SqsHelper();
        sqsHelper.setSqsClient(new AmazonSQSClient());
        return sqsHelper;
    }

    @Bean
    @Autowired
    public PollSqsWorker sqsWorker(BridgeWorkerPlatformSqsCallback callback) {
        Config config = bridgeConfig();

        PollSqsWorker sqsWorker = new PollSqsWorker();
        sqsWorker.setCallback(callback);
        sqsWorker.setQueueUrl(config.get("workerPlatform.request.sqs.queue.url"));
        sqsWorker.setSleepTimeMillis(config.getInt("workerPlatform.request.sqs.sleep.time.millis"));
        sqsWorker.setSqsHelper(sqsHelper());
        return sqsWorker;
    }

    @Bean(name="workerPlatformSynapseClient")
    public SynapseClient synapseClient() {
        SynapseClient synapseClient = new SynapseAdminClientImpl();
        synapseClient.setUsername(bridgeConfig().get("synapse.user"));
        synapseClient.setApiKey(bridgeConfig().get("synapse.api.key"));
        return synapseClient;
    }

    @Bean
    public SynapseHelper synapseHelper() {
        Config config = bridgeConfig();
        SynapseHelper synapseHelper = new SynapseHelper();
        synapseHelper.setAsyncIntervalMillis(config.getInt("synapse.poll.interval.millis"));
        synapseHelper.setAsyncTimeoutLoops(config.getInt("synapse.poll.max.tries"));
        synapseHelper.setGetColumnModelsRateLimit(
                config.getInt("synapse.get.column.models.rate.limit.per.minute") / 60.0);
        synapseHelper.setRateLimit(config.getInt("synapse.rate.limit.per.second"));
        synapseHelper.setSynapseClient(synapseClient());
        return synapseHelper;
    }

    @Bean(name="synapsePrincipalId")
    public long synapsePrincipalId() {
        return bridgeConfig().getInt("synapse.principal.id");
    }

    @Bean(name = Constants.SERVICE_TYPE_REPORTER)
    @Autowired
    public ThrowingConsumer<JsonNode> reporterWorker(BridgeReporterProcessor reporterProcessor) {
        return reporterProcessor::process;
    }

    @Bean(name = Constants.SERVICE_TYPE_UDD)
    @Autowired
    public ThrowingConsumer<JsonNode> uddWorker(BridgeUddProcessor uddProcessor) {
        return uddProcessor::process;
    }

    @PostConstruct
    public void initDynamoDbTables() throws InterruptedException {
        LOG.info("Initializing DynamoDB tables...");
        DynamoNamingHelper namingHelper = dynamoNamingHelper();
        AmazonDynamoDB ddbClient = AmazonDynamoDBClientBuilder.defaultClient();

        // Create tables.
        createTable(ddbClient, namingHelper, "FitBitTables",
                "studyId", ScalarAttributeType.S, "tableId", ScalarAttributeType.S);
        createTable(ddbClient, namingHelper, "NotificationConfig",
                "studyId", ScalarAttributeType.S, null, null);
        createTable(ddbClient, namingHelper, "NotificationLog",
                "userId", ScalarAttributeType.S, "notificationTime", ScalarAttributeType.N);
        createTable(ddbClient, namingHelper, "SynapseSurveyTables",
                "studyId", ScalarAttributeType.S, null, null);
        createTable(ddbClient, namingHelper, "WorkerLog",
                "workerId", ScalarAttributeType.S, "finishTime", ScalarAttributeType.N);

        // Wait for tables to be ready.
        waitForTable(ddbClient, namingHelper, "FitBitTables");
        waitForTable(ddbClient, namingHelper, "NotificationConfig");
        waitForTable(ddbClient, namingHelper, "NotificationLog");
        waitForTable(ddbClient, namingHelper, "SynapseSurveyTables");
        waitForTable(ddbClient, namingHelper, "WorkerLog");

        LOG.info("Finished initializing DynamoDB tables...");
    }

    private static void createTable(AmazonDynamoDB ddbClient, DynamoNamingHelper namingHelper, String tableName,
            String hashKeyName, ScalarAttributeType hashKeyType,
            String rangeKeyName, ScalarAttributeType rangeKeyType) {
        List<KeySchemaElement> keyList = new ArrayList<>();
        List<AttributeDefinition> attrList = new ArrayList<>();

        // Use the naming helper to resolve the table name.
        String resolvedTableName = namingHelper.getFullyQualifiedTableName(tableName);

        // Hash key.
        keyList.add(new KeySchemaElement(hashKeyName, KeyType.HASH));
        attrList.add(new AttributeDefinition(hashKeyName, hashKeyType));

        // Range key, if present.
        if (rangeKeyName != null) {
            keyList.add(new KeySchemaElement(rangeKeyName, KeyType.RANGE));
            attrList.add(new AttributeDefinition(rangeKeyName, rangeKeyType));
        }

        // Create table.
        CreateTableRequest req = new CreateTableRequest().withTableName(resolvedTableName).withKeySchema(keyList)
                .withAttributeDefinitions(attrList).withBillingMode(BillingMode.PAY_PER_REQUEST);
        TableUtils.createTableIfNotExists(ddbClient, req);
    }

    private static void waitForTable(AmazonDynamoDB ddbClient, DynamoNamingHelper namingHelper, String tableName)
            throws InterruptedException {
        TableUtils.waitUntilActive(ddbClient, namingHelper.getFullyQualifiedTableName(tableName));
    }
}
