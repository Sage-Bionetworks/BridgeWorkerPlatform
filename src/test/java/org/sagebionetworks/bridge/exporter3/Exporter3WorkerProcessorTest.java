package org.sagebionetworks.bridge.exporter3;

import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.sagebionetworks.bridge.exporter3.Exporter3WorkerProcessor.METADATA_KEY_INSTANCE_GUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.shiro.codec.Hex;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.sagebionetworks.repo.model.FileEntity;
import org.sagebionetworks.repo.model.annotation.v2.AnnotationsValue;
import org.sagebionetworks.repo.model.annotation.v2.AnnotationsValueType;
import org.sagebionetworks.repo.model.file.S3FileHandle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.crypto.CmsEncryptor;
import org.sagebionetworks.bridge.crypto.WrongEncryptionKeyException;
import org.sagebionetworks.bridge.exporter3.results.AssessmentResultSummarizer;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.AssessmentConfig;
import org.sagebionetworks.bridge.rest.model.ExportedRecordInfo;
import org.sagebionetworks.bridge.rest.model.ExportToAppNotification;
import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.TimelineMetadata;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.rest.model.UploadTableRow;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerRetryableException;
import org.sagebionetworks.bridge.synapse.SynapseHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.exceptions.WorkerException;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Exporter3WorkerProcessorTest {
    private static final String CLIENT_INFO = "dummy client info";
    private static final String CONTENT_TYPE = "text/plain";
    private static final String CUSTOM_METADATA_KEY = "custom-metadata-key";
    private static final String CUSTOM_METADATA_KEY_SANITIZED = "custom_metadata_key";
    private static final String CUSTOM_METADATA_VALUE = "custom-<b>metadata</b>-value";
    private static final String CUSTOM_METADATA_VALUE_CLEAN = "custom-metadata-value";
    private static final byte[] DUMMY_MD5_BYTES = "dummy-md5".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DUMMY_ENCRYPTED_FILE_BYTES = "dummy encrypted file content"
            .getBytes(StandardCharsets.UTF_8);
    private static final byte[] DUMMY_UNENCRYPTED_FILE_BYTES = "dummy unencrypted file content"
            .getBytes(StandardCharsets.UTF_8);
    private static final String ASSESSMENT_CONFIG = "{\n" +
            "  \"type\": \"assessment\",\n" +
            "  \"identifier\": \"xhcsds\",\n" +
            "  \"$schema\": \"https://sage-bionetworks.github.io/mobile-client-json/schemas/v2/AssessmentObject" +
            ".json\",\n" +
            "  \"versionString\": \"1.0.0\",\n" +
            "  \"estimatedMinutes\": 3,\n" +
            "  \"copyright\": \"Copyright © 2022 Sage Bionetworks. All rights reserved.\",\n" +
            "  \"title\": \"Example Survey A\",\n" +
            "  \"detail\": \"This is intended as an example of a survey with a list of questions. There are no " +
            "sections and there are no additional instructions. In this survey, pause navigation is hidden for all " +
            "nodes. For all questions, the skip button should say 'Skip me'. Default behavior is that buttons that " +
            "make logical sense to be displayed are shown unless they are explicitly hidden.\",\n" +
            "  \"steps\": [\n" +
            "    {\n" +
            "      \"type\": \"overview\",\n" +
            "      \"identifier\": \"overview\",\n" +
            "      \"title\": \"Example Survey A\",\n" +
            "      \"detail\": \"You will be shown a series of example questions. This survey has no additional " +
            "instructions.\",\n" +
            "      \"image\": {\n" +
            "        \"imageName\": \"day_to_day\",\n" +
            "        \"type\": \"sageResource\"\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"type\": \"choiceQuestion\",\n" +
            "      \"identifier\": \"choiceQ1\",\n" +
            "      \"comment\": \"Go to the question selected by the participant. If they skip the question then go " +
            "directly to follow-up.\",\n" +
            "      \"title\": \"Choose which question to answer\",\n" +
            "      \"surveyRules\": [\n" +
            "        {\n" +
            "          \"skipToIdentifier\": \"followupQ\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"matchingAnswer\": 1,\n" +
            "          \"skipToIdentifier\": \"simpleQ1\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"matchingAnswer\": 2,\n" +
            "          \"skipToIdentifier\": \"simpleQ2\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"matchingAnswer\": 3,\n" +
            "          \"skipToIdentifier\": \"simpleQ3\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"matchingAnswer\": 4,\n" +
            "          \"skipToIdentifier\": \"simpleQ4\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"matchingAnswer\": 5,\n" +
            "          \"skipToIdentifier\": \"simpleQ5\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"matchingAnswer\": 6,\n" +
            "          \"skipToIdentifier\": \"simpleQ6\"\n" +
            "        }\n" +
            "      ],\n" +
            "      \"baseType\": \"integer\",\n" +
            "      \"singleChoice\": true,\n" +
            "      \"choices\": [\n" +
            "        {\n" +
            "          \"value\": 1,\n" +
            "          \"text\": \"Enter some text\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"value\": 2,\n" +
            "          \"text\": \"Birth year\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"value\": 3,\n" +
            "          \"text\": \"Likert Scale\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"value\": 4,\n" +
            "          \"text\": \"Sliding Scale\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"value\": 5,\n" +
            "          \"text\": \"Duration\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"value\": 6,\n" +
            "          \"text\": \"Time\"\n" +
            "        }\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"type\": \"simpleQuestion\",\n" +
            "      \"identifier\": \"simpleQ1\",\n" +
            "      \"nextStepIdentifier\": \"followupQ\",\n" +
            "      \"title\": \"Enter some text\",\n" +
            "      \"inputItem\": {\n" +
            "        \"type\": \"string\",\n" +
            "        \"placeholder\": \"I like cake\"\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"type\": \"simpleQuestion\",\n" +
            "      \"identifier\": \"simpleQ2\",\n" +
            "      \"nextStepIdentifier\": \"followupQ\",\n" +
            "      \"title\": \"Enter a birth year\",\n" +
            "      \"inputItem\": {\n" +
            "        \"type\": \"year\",\n" +
            "        \"fieldLabel\": \"year of birth\",\n" +
            "        \"placeholder\": \"YYYY\",\n" +
            "        \"formatOptions\": {\n" +
            "          \"allowFuture\": false,\n" +
            "          \"minimumYear\": 1900\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"type\": \"simpleQuestion\",\n" +
            "      \"identifier\": \"simpleQ3\",\n" +
            "      \"nextStepIdentifier\": \"followupQ\",\n" +
            "      \"title\": \"How much do you like apples?\",\n" +
            "      \"uiHint\": \"likert\",\n" +
            "      \"inputItem\": {\n" +
            "        \"type\": \"integer\",\n" +
            "        \"formatOptions\": {\n" +
            "          \"maximumLabel\": \"Very much\",\n" +
            "          \"maximumValue\": 7,\n" +
            "          \"minimumLabel\": \"Not at all\",\n" +
            "          \"minimumValue\": 1\n" +
            "        }\n" +
            "      },\n" +
            "      \"shouldHideActions\": []\n" +
            "    },\n" +
            "    {\n" +
            "      \"type\": \"simpleQuestion\",\n" +
            "      \"identifier\": \"simpleQ4\",\n" +
            "      \"nextStepIdentifier\": \"followupQ\",\n" +
            "      \"title\": \"How much do you like apples?\",\n" +
            "      \"uiHint\": \"slider\",\n" +
            "      \"inputItem\": {\n" +
            "        \"type\": \"integer\",\n" +
            "        \"formatOptions\": {\n" +
            "          \"maximumLabel\": \"Very much\",\n" +
            "          \"maximumValue\": 100,\n" +
            "          \"minimumLabel\": \"Not at all\",\n" +
            "          \"minimumValue\": 0\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"type\": \"simpleQuestion\",\n" +
            "      \"identifier\": \"simpleQ5\",\n" +
            "      \"nextStepIdentifier\": \"followupQ\",\n" +
            "      \"title\": \"How long does it take to travel to the moon?\",\n" +
            "      \"inputItem\": {\n" +
            "        \"type\": \"duration\"\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"type\": \"simpleQuestion\",\n" +
            "      \"identifier\": \"simpleQ6\",\n" +
            "      \"nextStepIdentifier\": \"followupQ\",\n" +
            "      \"title\": \"What time is it on the moon?\",\n" +
            "      \"inputItem\": {\n" +
            "        \"type\": \"time\"\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"type\": \"choiceQuestion\",\n" +
            "      \"identifier\": \"followupQ\",\n" +
            "      \"title\": \"Are you happy with your choice?\",\n" +
            "      \"subtitle\": \"After thinking it over...\",\n" +
            "      \"detail\": \"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor " +
            "incididunt ut labore et dolore magna aliqua.\",\n" +
            "      \"surveyRules\": [\n" +
            "        {\n" +
            "          \"matchingAnswer\": false,\n" +
            "          \"skipToIdentifier\": \"choiceQ1\"\n" +
            "        }\n" +
            "      ],\n" +
            "      \"baseType\": \"boolean\",\n" +
            "      \"singleChoice\": true,\n" +
            "      \"choices\": [\n" +
            "        {\n" +
            "          \"value\": true,\n" +
            "          \"text\": \"Yes\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"value\": false,\n" +
            "          \"text\": \"No\"\n" +
            "        }\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"type\": \"choiceQuestion\",\n" +
            "      \"identifier\": \"favoriteFood\",\n" +
            "      \"title\": \"What are you having for dinner next Tuesday after the soccer game?\",\n" +
            "      \"subtitle\": \"After thinking it over...\",\n" +
            "      \"detail\": \"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor " +
            "incididunt ut labore et dolore magna aliqua.\",\n" +
            "      \"surveyRules\": [\n" +
            "        {\n" +
            "          \"matchingAnswer\": \"Pizza\",\n" +
            "          \"skipToIdentifier\": \"multipleChoice\",\n" +
            "          \"ruleOperator\": \"ne\"\n" +
            "        }\n" +
            "      ],\n" +
            "      \"baseType\": \"string\",\n" +
            "      \"singleChoice\": true,\n" +
            "      \"choices\": [\n" +
            "        {\n" +
            "          \"value\": \"Pizza\",\n" +
            "          \"text\": \"Pizza\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"value\": \"Sushi\",\n" +
            "          \"text\": \"Sushi\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"value\": \"Ice Cream\",\n" +
            "          \"text\": \"Ice Cream\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"value\": \"Beans & Rice\",\n" +
            "          \"text\": \"Beans & Rice\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"value\": \"Tofu Tacos\",\n" +
            "          \"text\": \"Tofu Tacos\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"value\": \"Bucatini Alla Carbonara\",\n" +
            "          \"text\": \"Bucatini Alla Carbonara\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"value\": \"Hot Dogs, Kraft Dinner & Potato Salad\",\n" +
            "          \"text\": \"Hot Dogs, Kraft Dinner & Potato Salad\"\n" +
            "        }\n" +
            "      ],\n" +
            "      \"other\": {\n" +
            "        \"type\": \"string\"\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"type\": \"instruction\",\n" +
            "      \"identifier\": \"pizza\",\n" +
            "      \"title\": \"Mmmmm, pizza...\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"type\": \"choiceQuestion\",\n" +
            "      \"identifier\": \"multipleChoice\",\n" +
            "      \"actions\": {\n" +
            "        \"goForward\": {\n" +
            "          \"buttonTitle\": \"Submit\",\n" +
            "          \"type\": \"default\"\n" +
            "        }\n" +
            "      },\n" +
            "      \"title\": \"What are your favorite colors?\",\n" +
            "      \"baseType\": \"string\",\n" +
            "      \"singleChoice\": false,\n" +
            "      \"choices\": [\n" +
            "        {\n" +
            "          \"value\": \"Blue\",\n" +
            "          \"text\": \"Blue\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"value\": \"Yellow\",\n" +
            "          \"text\": \"Yellow\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"value\": \"Red\",\n" +
            "          \"text\": \"Red\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"text\": \"All of the above\",\n" +
            "          \"selectorType\": \"all\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"text\": \"None of the above\",\n" +
            "          \"selectorType\": \"exclusive\"\n" +
            "        }\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"type\": \"completion\",\n" +
            "      \"identifier\": \"completion\",\n" +
            "      \"title\": \"You're done!\"\n" +
            "    }\n" +
            "  ],\n" +
            "  \"shouldHideActions\": [],\n" +
            "  \"webConfig\": {\n" +
            "    \"skipOption\": \"SKIP\"\n" +
            "  },\n" +
            "  \"interruptionHandling\": {\n" +
            "    \"canResume\": true,\n" +
            "    \"reviewIdentifier\": \"beginning\",\n" +
            "    \"canSkip\": true,\n" +
            "    \"canSaveForLater\": true\n" +
            "  }\n" +
            "}";
    private static final String DUMMY_ASSESSMENT_RESULTS = "{\n" +
            "   \"type\":\"assessment\",\n" +
            "   \"identifier\":\"xhcsds\",\n" +
            "   \"assessmentIdentifier\":\"xhcsds\",\n" +
            "   \"schemaIdentifier\":null,\n" +
            "   \"versionString\":\"1.0.0\",\n" +
            "   \"stepHistory\":[\n" +
            "      {\n" +
            "         \"type\":\"base\",\n" +
            "         \"identifier\":\"overview\",\n" +
            "         \"startDate\":\"2023-11-02T09:41:50.983-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:41:52.635-07:00\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"choiceQ1\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"integer\"\n" +
            "         },\n" +
            "         \"value\":1,\n" +
            "         \"startDate\":\"2023-11-02T09:41:52.635-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:41:54.278-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"simpleQ1\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"string\"\n" +
            "         },\n" +
            "         \"value\":\"test text\",\n" +
            "         \"startDate\":\"2023-11-02T09:41:54.278-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:00.516-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"followupQ\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"boolean\"\n" +
            "         },\n" +
            "         \"value\":false,\n" +
            "         \"startDate\":\"2023-11-02T09:42:00.516-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:02.357-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"choiceQ1\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"integer\"\n" +
            "         },\n" +
            "         \"value\":2,\n" +
            "         \"startDate\":\"2023-11-02T09:42:02.357-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:03.439-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"simpleQ2\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"integer\"\n" +
            "         },\n" +
            "         \"value\":1945,\n" +
            "         \"startDate\":\"2023-11-02T09:42:03.439-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:07.803-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"followupQ\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"boolean\"\n" +
            "         },\n" +
            "         \"value\":false,\n" +
            "         \"startDate\":\"2023-11-02T09:42:07.803-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:08.663-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"choiceQ1\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"integer\"\n" +
            "         },\n" +
            "         \"value\":3,\n" +
            "         \"startDate\":\"2023-11-02T09:42:08.663-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:09.864-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"simpleQ3\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"integer\"\n" +
            "         },\n" +
            "         \"value\":4,\n" +
            "         \"startDate\":\"2023-11-02T09:42:09.865-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:11.051-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"followupQ\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"boolean\"\n" +
            "         },\n" +
            "         \"value\":false,\n" +
            "         \"startDate\":\"2023-11-02T09:42:11.052-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:12.076-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"choiceQ1\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"integer\"\n" +
            "         },\n" +
            "         \"value\":4,\n" +
            "         \"startDate\":\"2023-11-02T09:42:12.076-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:13.243-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"simpleQ4\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"integer\"\n" +
            "         },\n" +
            "         \"value\":50,\n" +
            "         \"startDate\":\"2023-11-02T09:42:13.243-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:21.662-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"followupQ\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"boolean\"\n" +
            "         },\n" +
            "         \"value\":false,\n" +
            "         \"startDate\":\"2023-11-02T09:42:21.662-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:22.496-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"choiceQ1\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"integer\"\n" +
            "         },\n" +
            "         \"value\":5,\n" +
            "         \"startDate\":\"2023-11-02T09:42:22.496-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:23.773-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"simpleQ5\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"duration\",\n" +
            "            \"displayUnits\":[\n" +
            "               \"hour\",\n" +
            "               \"minute\"\n" +
            "            ],\n" +
            "            \"significantDigits\":0\n" +
            "         },\n" +
            "         \"value\":28800.0,\n" +
            "         \"startDate\":\"2023-11-02T09:42:23.773-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:29.907-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"followupQ\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"boolean\"\n" +
            "         },\n" +
            "         \"value\":false,\n" +
            "         \"startDate\":\"2023-11-02T09:42:29.907-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:31.462-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"choiceQ1\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"integer\"\n" +
            "         },\n" +
            "         \"value\":6,\n" +
            "         \"startDate\":\"2023-11-02T09:42:31.462-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:32.779-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"simpleQ6\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"time\",\n" +
            "            \"codingFormat\":\"HH:mm:SS.SSS\"\n" +
            "         },\n" +
            "         \"value\":\"09:42\",\n" +
            "         \"startDate\":\"2023-11-02T09:42:32.779-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:36.374-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"followupQ\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"boolean\"\n" +
            "         },\n" +
            "         \"value\":true,\n" +
            "         \"startDate\":\"2023-11-02T09:42:36.374-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:39.175-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"favoriteFood\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"string\"\n" +
            "         },\n" +
            "         \"value\":\"Pizza\",\n" +
            "         \"startDate\":\"2023-11-02T09:42:39.175-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:42.061-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"base\",\n" +
            "         \"identifier\":\"pizza\",\n" +
            "         \"startDate\":\"2023-11-02T09:42:42.061-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:42.957-07:00\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"answer\",\n" +
            "         \"identifier\":\"multipleChoice\",\n" +
            "         \"answerType\":{\n" +
            "            \"type\":\"array\",\n" +
            "            \"baseType\":\"string\",\n" +
            "            \"sequenceSeparator\":null\n" +
            "         },\n" +
            "         \"value\":[\n" +
            "            \"Blue\",\n" +
            "            \"Yellow\",\n" +
            "            \"Red\"\n" +
            "         ],\n" +
            "         \"startDate\":\"2023-11-02T09:42:42.957-07:00\",\n" +
            "         \"endDate\":\"2023-11-02T09:42:45.999-07:00\",\n" +
            "         \"questionText\":null,\n" +
            "         \"questionData\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"type\":\"base\",\n" +
            "         \"identifier\":\"completion\",\n" +
            "         \"startDate\":\"2023-11-02T09:42:45.999-07:00\",\n" +
            "         \"endDate\":null\n" +
            "      }\n" +
            "   ],\n" +
            "   \"asyncResults\":[\n" +
            "      \n" +
            "   ],\n" +
            "   \"taskRunUUID\":\"b4bf1453-ec1b-4247-840f-2cf6ffaeb8b6\",\n" +
            "   \"startDate\":\"2023-11-02T09:41:50.983-07:00\",\n" +
            "   \"endDate\":\"2023-11-02T09:42:45.999-07:00\",\n" +
            "   \"path\":[\n" +
            "      {\n" +
            "         \"identifier\":\"overview\",\n" +
            "         \"direction\":\"forward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"choiceQ1\",\n" +
            "         \"direction\":\"forward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"simpleQ1\",\n" +
            "         \"direction\":\"forward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"followupQ\",\n" +
            "         \"direction\":\"forward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"choiceQ1\",\n" +
            "         \"direction\":\"backward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"simpleQ2\",\n" +
            "         \"direction\":\"forward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"followupQ\",\n" +
            "         \"direction\":\"forward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"choiceQ1\",\n" +
            "         \"direction\":\"backward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"simpleQ3\",\n" +
            "         \"direction\":\"forward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"followupQ\",\n" +
            "         \"direction\":\"forward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"choiceQ1\",\n" +
            "         \"direction\":\"backward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"simpleQ4\",\n" +
            "         \"direction\":\"forward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"followupQ\",\n" +
            "         \"direction\":\"forward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"choiceQ1\",\n" +
            "         \"direction\":\"backward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"simpleQ5\",\n" +
            "         \"direction\":\"forward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"followupQ\",\n" +
            "         \"direction\":\"forward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"choiceQ1\",\n" +
            "         \"direction\":\"backward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"simpleQ6\",\n" +
            "         \"direction\":\"forward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"followupQ\",\n" +
            "         \"direction\":\"forward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"favoriteFood\",\n" +
            "         \"direction\":\"forward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"pizza\",\n" +
            "         \"direction\":\"forward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"multipleChoice\",\n" +
            "         \"direction\":\"forward\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"identifier\":\"completion\",\n" +
            "         \"direction\":\"forward\"\n" +
            "      }\n" +
            "   ],\n" +
            "   \"$schema\":\"https://sage-bionetworks.github.io/mobile-client-json/schemas/v2/AssessmentResultObject" +
            ".json\"\n" +
            "}";
    private static final String EXPORTED_FILE_ENTITY_ID = "syn2222";
    private static final String EXPORTED_FILE_HANDLE_ID = "3333";
    private static final String FILENAME = "filename.txt";
    private static final String HEALTH_CODE = "health-code";
    private static final int PARTICIPANT_VERSION = 42;
    private static final String RAW_DATA_BUCKET = "raw-data-bucket";
    private static final String RECORD_ID = "test-record";
    private static final String SCHEDULE_GUID = "test-schedule-guid";
    private static final String TODAYS_DATE_STRING = "2021-08-23";
    private static final String TODAYS_FOLDER_ID = "syn7777";
    private static final String UPLOAD_BUCKET = "upload-bucket";
    private static final String USER_AGENT = "dummy user agent";
    private static final String INSTANCE_GUID = "instanceGuid";
    private static final String ASSESSMENT_INSTANCE_GUID = "assessmentInstanceGuid";
    private static final String ASSESSMENT_GUID = "assessmentGuid";
    private static final String ASSESSMENT_ID = "assessmentId";
    private static final String SESSION_GUID = "session-guid";
    
    private static final long MOCK_NOW_MILLIS = DateTime.parse(TODAYS_DATE_STRING + "T15:32:38.914-0700")
            .getMillis();
    private static final DateTime UPLOADED_ON = DateTime.parse(TODAYS_DATE_STRING + "T15:27:28.647-0700");
    private static final long UPLOADED_ON_MILLIS = UPLOADED_ON.getMillis();

    private static final String FULL_FILENAME = RECORD_ID + '-' + FILENAME;
    private static final String EXPECTED_S3_KEY = Exporter3TestUtil.APP_ID + '/' + TODAYS_DATE_STRING + '/' + FULL_FILENAME;
    private static final String EXPECTED_S3_KEY_FOR_STUDY = Exporter3TestUtil.APP_ID + '/' + Exporter3TestUtil.STUDY_ID + '/' + TODAYS_DATE_STRING + '/' +
            FULL_FILENAME;

    private static class EmptyCacheLoader extends CacheLoader<String, CmsEncryptor> {
        public static final LoadingCache<String, CmsEncryptor> LOADING_CACHE_INSTANCE = CacheBuilder.newBuilder()
                .build(new EmptyCacheLoader());

        @Override
        public CmsEncryptor load(String appId) {
            return null;
        }
    }

    private static class SingletonCacheLoader extends CacheLoader<String, CmsEncryptor> {
        private final CmsEncryptor encryptor;

        public static LoadingCache<String, CmsEncryptor> makeLoadingCache(CmsEncryptor encryptor) {
            return CacheBuilder.newBuilder().build(new SingletonCacheLoader(encryptor));
        }

        private SingletonCacheLoader(CmsEncryptor encryptor) {
            this.encryptor = encryptor;
        }

        @Override
        public CmsEncryptor load(String appId) {
            return encryptor;
        }
    }

    private static class ThrowingCacheLoader extends CacheLoader<String, CmsEncryptor> {
        public static final LoadingCache<String, CmsEncryptor> LOADING_CACHE_INSTANCE = CacheBuilder.newBuilder()
                .build(new ThrowingCacheLoader());

        @Override
        public CmsEncryptor load(String appId) {
            throw new RuntimeException("test exception");
        }
    }

    private InMemoryFileHelper inMemoryFileHelper;
    private byte[] writtenToS3;

    @Mock
    private BridgeHelper mockBridgeHelper;

    @Mock
    private DigestUtils mockDigestUtils;

    @Mock
    private S3Helper mockS3Helper;

    @Mock
    private SynapseHelper mockSynapseHelper;

    @InjectMocks
    @Spy
    private Exporter3WorkerProcessor processor;

    @BeforeClass
    public static void beforeClass() {
        DateTimeUtils.setCurrentMillisFixed(MOCK_NOW_MILLIS);
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        MockitoAnnotations.initMocks(this);

        // Use InMemoryFileHelper for FileHelper.
        inMemoryFileHelper = new InMemoryFileHelper();
        processor.setFileHelper(inMemoryFileHelper);

        // Mock config.
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(Exporter3WorkerProcessor.CONFIG_KEY_RAW_HEALTH_DATA_BUCKET)).thenReturn(RAW_DATA_BUCKET);
        when(mockConfig.get(Exporter3WorkerProcessor.CONFIG_KEY_UPLOAD_BUCKET)).thenReturn(UPLOAD_BUCKET);
        processor.setBridgeConfig(mockConfig);

        // Mock SynapseHelper.isSynapseWritable().
        when(mockSynapseHelper.isSynapseWritable()).thenReturn(true);

        // Reset byte array.
        writtenToS3 = null;
    }

    @AfterClass
    public static void afterClass() {
        DateTimeUtils.setCurrentMillisSystem();
    }

    @Test
    public void accept() throws Exception {
        // For branch coverage, test parsing the request and passing it to process().
        // Spy process().
        doNothing().when(processor).process(any());

        // Set up inputs. Exporter3Request deserialization is tested elsewhere.
        JsonNode requestNode = DefaultObjectMapper.INSTANCE.convertValue(makeRequest(), JsonNode.class);

        // Execute and verify.
        processor.accept(requestNode);

        ArgumentCaptor<Exporter3Request> requestArgumentCaptor = ArgumentCaptor.forClass(Exporter3Request.class);
        verify(processor).process(requestArgumentCaptor.capture());

        Exporter3Request capturedRequest = requestArgumentCaptor.getValue();
        assertEquals(capturedRequest.getAppId(), Exporter3TestUtil.APP_ID);
        assertEquals(capturedRequest.getRecordId(), RECORD_ID);
    }

    @Test(expectedExceptions = PollSqsWorkerRetryableException.class)
    public void synapseNotWritable() throws Exception {
        // Mock services.
        when(mockSynapseHelper.isSynapseWritable()).thenReturn(false);

        // Execute.
        processor.process(makeRequest());
    }

    @Test
    public void ex3EnabledNull() throws Exception {
        // Mock services.
        App app = Exporter3TestUtil.makeAppWithEx3Config();
        app.setExporter3Enabled(null);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        // Execute.
        processor.process(makeRequest());

        // Verify calls to services.
        verify(mockSynapseHelper).isSynapseWritable();
        verify(mockBridgeHelper).getApp(Exporter3TestUtil.APP_ID);

        // Verify no more interactions with backend services.
        verifyNoMoreInteractions(mockBridgeHelper, mockSynapseHelper);
    }

    @Test
    public void ex3EnabledFalse() throws Exception {
        // Mock services.
        App app = Exporter3TestUtil.makeAppWithEx3Config();
        app.setExporter3Enabled(false);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        // Execute.
        processor.process(makeRequest());

        // Verify calls to services.
        verify(mockSynapseHelper).isSynapseWritable();
        verify(mockBridgeHelper).getApp(Exporter3TestUtil.APP_ID);

        // Verify no more interactions with backend services.
        verifyNoMoreInteractions(mockBridgeHelper, mockSynapseHelper);
    }

    @Test
    public void recordNoSharing() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());

        HealthDataRecordEx3 record = makeRecord();
        record.setSharingScope(SharingScope.NO_SHARING);
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(record);

        // Execute.
        processor.process(makeRequest());

        // Verify calls to services.
        verify(mockSynapseHelper).isSynapseWritable();
        verify(mockBridgeHelper).getApp(Exporter3TestUtil.APP_ID);
        verify(mockBridgeHelper).getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID);

        // Verify no more interactions with backend services.
        verifyNoMoreInteractions(mockBridgeHelper, mockSynapseHelper);
    }

    @Test
    public void participantNoSharing() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());

        StudyParticipant participant = mockParticipant();
        when(participant.getSharingScope()).thenReturn(SharingScope.NO_SHARING);
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(participant);

        // Execute.
        processor.process(makeRequest());

        // Verify calls to services.
        verify(mockSynapseHelper).isSynapseWritable();
        verify(mockBridgeHelper).getApp(Exporter3TestUtil.APP_ID);
        verify(mockBridgeHelper).getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID);
        verify(mockBridgeHelper).getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false);

        // Verify no more interactions with backend services.
        verifyNoMoreInteractions(mockBridgeHelper, mockSynapseHelper);
    }

    @Test(expectedExceptions = WorkerException.class)
    public void encryptedUpload_ErrorGettingEncryptor() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());
        StudyParticipant mockParticipant = mockParticipant();
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        Upload mockUpload = mockUpload(true);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        processor.setCmsEncryptorCache(ThrowingCacheLoader.LOADING_CACHE_INSTANCE);

        // Execute.
        processor.process(makeRequest());
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void encryptedUpload_EncryptorNotFound() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());
        StudyParticipant mockParticipant = mockParticipant();
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        Upload mockUpload = mockUpload(true);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        processor.setCmsEncryptorCache(EmptyCacheLoader.LOADING_CACHE_INSTANCE);

        // Execute.
        processor.process(makeRequest());
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void encryptedUpload_WrongEncryptionKey() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());
        StudyParticipant mockParticipant = mockParticipant();
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        Upload mockUpload = mockUpload(true);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        doAnswer(invocation -> {
            File file = invocation.getArgumentAt(2, File.class);
            inMemoryFileHelper.writeBytes(file, DUMMY_ENCRYPTED_FILE_BYTES);
            return null;
        }).when(mockS3Helper).downloadS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), any());

        CmsEncryptor mockEncryptor = mock(CmsEncryptor.class);
        when(mockEncryptor.decrypt(any(InputStream.class))).thenThrow(WrongEncryptionKeyException.class);
        processor.setCmsEncryptorCache(SingletonCacheLoader.makeLoadingCache(mockEncryptor));

        // Don't actually buffer the input stream, as this breaks the test.
        doAnswer(invocation -> invocation.getArgumentAt(0, InputStream.class)).when(processor)
                .getBufferedInputStream(any());

        // Execute.
        processor.process(makeRequest());
    }

    @Test
    public void encryptedUpload() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());
        StudyParticipant mockParticipant = mockParticipant();
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);
        when(mockDigestUtils.digest(any(File.class))).thenReturn(DUMMY_MD5_BYTES);

        Upload mockUpload = mockUpload(true);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        doAnswer(invocation -> {
            File file = invocation.getArgumentAt(2, File.class);
            inMemoryFileHelper.writeBytes(file, DUMMY_ENCRYPTED_FILE_BYTES);
            return null;
        }).when(mockS3Helper).downloadS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), any());

        CmsEncryptor mockEncryptor = mock(CmsEncryptor.class);
        when(mockEncryptor.decrypt(any(InputStream.class))).thenReturn(new ByteArrayInputStream(
                DUMMY_UNENCRYPTED_FILE_BYTES));
        processor.setCmsEncryptorCache(SingletonCacheLoader.makeLoadingCache(mockEncryptor));

        // Don't actually buffer the input stream, as this breaks the test.
        doAnswer(invocation -> invocation.getArgumentAt(0, InputStream.class)).when(processor)
                .getBufferedInputStream(any());

        doAnswer(invocation -> {
            File file = invocation.getArgumentAt(2, File.class);
            writtenToS3 = inMemoryFileHelper.getBytes(file);
            return null;
        }).when(mockS3Helper).writeFileToS3(eq(RAW_DATA_BUCKET), eq(EXPECTED_S3_KEY), any(), any());

        mockSynapseHelper();

        // Execute.
        processor.process(makeRequest());

        // Verify services.
        verify(mockS3Helper).downloadS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), any());
        
        // This isn't called because there's no instanceGuid in the user's metadata map
        verify(mockBridgeHelper, never()).getTimelineMetadata(any(), any());

        ArgumentCaptor<InputStream> encryptedInputStreamCaptor = ArgumentCaptor.forClass(InputStream.class);
        verify(mockEncryptor).decrypt(encryptedInputStreamCaptor.capture());
        InputStream encryptedInputStream = encryptedInputStreamCaptor.getValue();
        assertEquals(ByteStreams.toByteArray(encryptedInputStream), DUMMY_ENCRYPTED_FILE_BYTES);

        ArgumentCaptor<ObjectMetadata> s3MetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
        verify(mockS3Helper).writeFileToS3(eq(RAW_DATA_BUCKET), eq(EXPECTED_S3_KEY), any(),
                s3MetadataCaptor.capture());
        assertEquals(writtenToS3, DUMMY_UNENCRYPTED_FILE_BYTES);
        verifyS3Metadata(s3MetadataCaptor.getValue());

        verifySynapseExport(EXPECTED_S3_KEY);
        verifyUpdatedRecordForApp();
        verifyExportNotificationForApp();
    }

    @Test
    public void nonEncryptedUpload() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());
        StudyParticipant mockParticipant = mockParticipant();
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        Upload mockUpload = mockUpload(false);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        mockSynapseHelper();

        // Execute.
        processor.process(makeRequest());

        // Verify services.
        ArgumentCaptor<ObjectMetadata> s3MetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
        verify(mockS3Helper).copyS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), eq(RAW_DATA_BUCKET), eq(EXPECTED_S3_KEY),
                s3MetadataCaptor.capture());
        verifyS3Metadata(s3MetadataCaptor.getValue());

        verifySynapseExport(EXPECTED_S3_KEY);
        verifyUpdatedRecordForApp();
        verifyExportNotificationForApp();
    }

    @Test
    public void fileAlreadyExists() throws Exception {
        // Mock services.
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());
        StudyParticipant mockParticipant = mockParticipant();
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        Upload mockUpload = mockUpload(false);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        when(mockSynapseHelper.createFolderIfNotExists(Exporter3TestUtil.RAW_FOLDER_ID, TODAYS_DATE_STRING))
                .thenReturn(TODAYS_FOLDER_ID);

        S3FileHandle createdFileHandle = new S3FileHandle();
        createdFileHandle.setId(EXPORTED_FILE_HANDLE_ID);
        when(mockSynapseHelper.createS3FileHandleWithRetry(any())).thenReturn(createdFileHandle);

        when(mockSynapseHelper.lookupChildWithRetry(TODAYS_FOLDER_ID, FULL_FILENAME))
                .thenReturn(EXPORTED_FILE_ENTITY_ID);

        FileEntity existingFileEntity = new FileEntity();
        existingFileEntity.setId(EXPORTED_FILE_ENTITY_ID);
        when(mockSynapseHelper.getEntityWithRetry(EXPORTED_FILE_ENTITY_ID, FileEntity.class))
                .thenReturn(existingFileEntity);

        FileEntity updateFileEntity = new FileEntity();
        updateFileEntity.setId(EXPORTED_FILE_ENTITY_ID);
        when(mockSynapseHelper.updateEntityWithRetry(any(FileEntity.class))).thenReturn(updateFileEntity);

        // Execute.
        processor.process(makeRequest());

        // Just verify call to updateEntity().
        ArgumentCaptor<FileEntity> fileEntityCaptor = ArgumentCaptor.forClass(FileEntity.class);
        verify(mockSynapseHelper).updateEntityWithRetry(fileEntityCaptor.capture());

        FileEntity fileEntity = fileEntityCaptor.getValue();
        assertEquals(fileEntity.getDataFileHandleId(), EXPORTED_FILE_HANDLE_ID);
        assertEquals(fileEntity.getName(), FULL_FILENAME);
        assertEquals(fileEntity.getParentId(), TODAYS_FOLDER_ID);
    }

    @Test
    public void schedulingMetadataAddedToExport() throws Exception {
        TimelineMetadata meta = mock(TimelineMetadata.class);
        when(meta.getMetadata()).thenReturn(ImmutableMap.of(
                "assessmentInstanceGuid", ASSESSMENT_INSTANCE_GUID, "sessionGuid", SESSION_GUID));
        when(mockBridgeHelper.getTimelineMetadata(Exporter3TestUtil.APP_ID, INSTANCE_GUID)).thenReturn(meta);

        HealthDataRecordEx3 record = makeRecord();
        record.putMetadataItem(METADATA_KEY_INSTANCE_GUID, INSTANCE_GUID);

        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(record);
        StudyParticipant mockParticipant = mockParticipant();
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);
        when(mockDigestUtils.digest(any(File.class))).thenReturn(DUMMY_MD5_BYTES);

        Upload mockUpload = mockUpload(true);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        doAnswer(invocation -> {
            File file = invocation.getArgumentAt(2, File.class);
            inMemoryFileHelper.writeBytes(file, DUMMY_ENCRYPTED_FILE_BYTES);
            return null;
        }).when(mockS3Helper).downloadS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), any());

        CmsEncryptor mockEncryptor = mock(CmsEncryptor.class);
        when(mockEncryptor.decrypt(any(InputStream.class))).thenReturn(new ByteArrayInputStream(
                DUMMY_UNENCRYPTED_FILE_BYTES));
        processor.setCmsEncryptorCache(SingletonCacheLoader.makeLoadingCache(mockEncryptor));

        // Don't actually buffer the input stream, as this breaks the test.
        doAnswer(invocation -> invocation.getArgumentAt(0, InputStream.class)).when(processor)
                .getBufferedInputStream(any());

        doAnswer(invocation -> {
            File file = invocation.getArgumentAt(2, File.class);
            writtenToS3 = inMemoryFileHelper.getBytes(file);
            return null;
        }).when(mockS3Helper).writeFileToS3(eq(RAW_DATA_BUCKET), eq(EXPECTED_S3_KEY), any(), any());

        mockSynapseHelper();

        // Execute.
        processor.process(makeRequest());
        
        ArgumentCaptor<ObjectMetadata> s3MetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
        verify(mockS3Helper).writeFileToS3(eq(RAW_DATA_BUCKET), eq(EXPECTED_S3_KEY), any(),
                s3MetadataCaptor.capture());
        
        Map<String, String> userMetadataMap = s3MetadataCaptor.getValue().getUserMetadata();
        assertEquals(userMetadataMap.get("assessmentInstanceGuid"), ASSESSMENT_INSTANCE_GUID);
        assertEquals(userMetadataMap.get("sessionGuid"), SESSION_GUID);
    }

    @Test
    public void uploadTableRow() throws Exception {
        //Mock getAssessment
        Assessment assessment = new Assessment().title(ASSESSMENT_ID).osName("Universal").ownerId("sage-bionetworks")
                .identifier(ASSESSMENT_ID).guid(ASSESSMENT_GUID).frameworkIdentifier(
                        AssessmentResultSummarizer.FRAMEWORK_IDENTIFIER);
        when(mockBridgeHelper.getAssessment(any(), eq(ASSESSMENT_GUID))).thenReturn(assessment);
        // Mock getAssessmentConfig
        AssessmentConfig assessmentConfig = new AssessmentConfig();
        assessmentConfig.setConfig(ASSESSMENT_CONFIG);

        // Mock extractFileFromZip to return assessmentResult.json file
        doAnswer(invocation -> {
            File file = inMemoryFileHelper.newFile(inMemoryFileHelper.createTempDir(), "assessmentResult.json");
            inMemoryFileHelper.writeBytes(file, DUMMY_ASSESSMENT_RESULTS.getBytes(StandardCharsets.UTF_8));
            Mockito.when(file.exists()).thenReturn(true);
            return file;
        }).when(processor).extractFileFromZip(any(), any(), eq("assessmentResult.json"));

        doAnswer(invocation -> {
            File file = invocation.getArgumentAt(2, File.class);
            inMemoryFileHelper.writeBytes(file, DUMMY_ENCRYPTED_FILE_BYTES);
            return null;
        }).when(mockS3Helper).downloadS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), any());

        when(mockBridgeHelper.getAssessmentConfig(any(), eq(ASSESSMENT_GUID))).thenReturn(assessmentConfig);

        Upload upload = mockUpload(true);
        HealthDataRecordEx3 record = makeRecord();
        record.putMetadataItem(METADATA_KEY_INSTANCE_GUID, INSTANCE_GUID);
        Map<String, String> metadataMap = ImmutableMap.of(
                "assessmentGuid", ASSESSMENT_GUID,
                "instanceGuid", ASSESSMENT_INSTANCE_GUID,
                "sessionGuid", SESSION_GUID);

        UploadTableRow tableRow = processor.getUploadTableRow(upload, record, RECORD_ID, metadataMap);
        assertNotNull(tableRow);
        assertEquals(RECORD_ID, tableRow.getRecordId());
        assertEquals(ASSESSMENT_GUID, tableRow.getAssessmentGuid());
        assertEquals(HEALTH_CODE, tableRow.getHealthCode());
        assertEquals(PARTICIPANT_VERSION, tableRow.getParticipantVersion().intValue());

        // Check metadata values
        ImmutableMap.Builder<String,String> expectedMetaBuilder = ImmutableMap.builder();
        expectedMetaBuilder.put("clientInfo", CLIENT_INFO);
        expectedMetaBuilder.put("sessionGuid", SESSION_GUID);
        Map<String, String> expectedMetaResults = expectedMetaBuilder.build();
        assertEquals(expectedMetaResults, tableRow.getMetadata());

        // Check data values
        ImmutableMap.Builder<String,String> builder = ImmutableMap.builder();
        builder.put("choiceQ1", "6");
        builder.put("simpleQ1", "test text");
        builder.put("followupQ", "true");
        builder.put("simpleQ2", "1945");
        builder.put("simpleQ3", "4");
        builder.put("simpleQ4", "50");
        builder.put("simpleQ5", "28800.0");
        builder.put("simpleQ6", "09:42");
        builder.put("favoriteFood", "Pizza");
        builder.put("multipleChoice", "Blue,Yellow,Red");
        Map<String, String> expectedDataResults = builder.build();
        assertEquals(expectedDataResults, tableRow.getData());

    }

    @Test
    public void encryptedUploadForStudy() throws Exception {
        // Mock services.
        App app = new App();
        app.setIdentifier(Exporter3TestUtil.APP_ID);
        app.setExporter3Enabled(true);
        app.setExporter3Configuration(null);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        StudyParticipant mockParticipant = mockParticipant();
        when(mockParticipant.getStudyIds()).thenReturn(ImmutableList.of(Exporter3TestUtil.STUDY_ID));
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, Exporter3TestUtil.STUDY_ID)).thenReturn(Exporter3TestUtil.makeStudyWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());
        when(mockDigestUtils.digest(any(File.class))).thenReturn(DUMMY_MD5_BYTES);

        Upload mockUpload = mockUpload(true);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        doAnswer(invocation -> {
            File file = invocation.getArgumentAt(2, File.class);
            inMemoryFileHelper.writeBytes(file, DUMMY_ENCRYPTED_FILE_BYTES);
            return null;
        }).when(mockS3Helper).downloadS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), any());

        CmsEncryptor mockEncryptor = mock(CmsEncryptor.class);
        when(mockEncryptor.decrypt(any(InputStream.class))).thenReturn(new ByteArrayInputStream(
                DUMMY_UNENCRYPTED_FILE_BYTES));
        processor.setCmsEncryptorCache(SingletonCacheLoader.makeLoadingCache(mockEncryptor));

        // Don't actually buffer the input stream, as this breaks the test.
        doAnswer(invocation -> invocation.getArgumentAt(0, InputStream.class)).when(processor)
                .getBufferedInputStream(any());

        doAnswer(invocation -> {
            File file = invocation.getArgumentAt(2, File.class);
            writtenToS3 = inMemoryFileHelper.getBytes(file);
            return null;
        }).when(mockS3Helper).writeFileToS3(eq(RAW_DATA_BUCKET), eq(EXPECTED_S3_KEY_FOR_STUDY), any(), any());

        mockSynapseHelper();

        // Execute.
        processor.process(makeRequest());

        // Verify services.
        verify(mockS3Helper).downloadS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), any());

        // This isn't called because there's no instanceGuid in the user's metadata map
        verify(mockBridgeHelper, never()).getTimelineMetadata(any(), any());

        ArgumentCaptor<InputStream> encryptedInputStreamCaptor = ArgumentCaptor.forClass(InputStream.class);
        verify(mockEncryptor).decrypt(encryptedInputStreamCaptor.capture());
        InputStream encryptedInputStream = encryptedInputStreamCaptor.getValue();
        assertEquals(ByteStreams.toByteArray(encryptedInputStream), DUMMY_ENCRYPTED_FILE_BYTES);

        ArgumentCaptor<ObjectMetadata> s3MetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
        verify(mockS3Helper).writeFileToS3(eq(RAW_DATA_BUCKET), eq(EXPECTED_S3_KEY_FOR_STUDY), any(),
                s3MetadataCaptor.capture());
        assertEquals(writtenToS3, DUMMY_UNENCRYPTED_FILE_BYTES);
        verifyS3Metadata(s3MetadataCaptor.getValue());

        verifySynapseExport(EXPECTED_S3_KEY_FOR_STUDY);
        verifyUpdatedRecordForStudy();
        verifyExportNotificationForStudy();
    }

    @Test
    public void nonEncryptedUploadForStudy() throws Exception {
        // Mock services.
        App app = new App();
        app.setIdentifier(Exporter3TestUtil.APP_ID);
        app.setExporter3Enabled(true);
        app.setExporter3Configuration(null);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        StudyParticipant mockParticipant = mockParticipant();
        when(mockParticipant.getStudyIds()).thenReturn(ImmutableList.of(Exporter3TestUtil.STUDY_ID));
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, Exporter3TestUtil.STUDY_ID)).thenReturn(Exporter3TestUtil.makeStudyWithEx3Config());
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());

        Upload mockUpload = mockUpload(false);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        mockSynapseHelper();

        // Execute.
        processor.process(makeRequest());

        // Verify services.
        ArgumentCaptor<ObjectMetadata> s3MetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
        verify(mockS3Helper).copyS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), eq(RAW_DATA_BUCKET),
                eq(EXPECTED_S3_KEY_FOR_STUDY), s3MetadataCaptor.capture());
        verifyS3Metadata(s3MetadataCaptor.getValue());

        verifySynapseExport(EXPECTED_S3_KEY_FOR_STUDY);
        verifyUpdatedRecordForStudy();
        verifyExportNotificationForStudy();
    }

    @Test
    public void nonEncryptedUploadForStudyFromTimeline() throws Exception {
        // Mock services.
        App app = new App();
        app.setIdentifier(Exporter3TestUtil.APP_ID);
        app.setExporter3Enabled(true);
        app.setExporter3Configuration(null);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        StudyParticipant mockParticipant = mockParticipant();
        when(mockParticipant.getStudyIds()).thenReturn(ImmutableList.of(Exporter3TestUtil.STUDY_ID, "non-timeline-study"));
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        TimelineMetadata meta = mock(TimelineMetadata.class);
        when(meta.getMetadata()).thenReturn(ImmutableMap.of(Exporter3WorkerProcessor.METADATA_KEY_SCHEDULE_GUID,
                SCHEDULE_GUID));
        when(mockBridgeHelper.getTimelineMetadata(Exporter3TestUtil.APP_ID, INSTANCE_GUID)).thenReturn(meta);

        HealthDataRecordEx3 record = makeRecord();
        record.putMetadataItem(METADATA_KEY_INSTANCE_GUID, INSTANCE_GUID);
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(record);

        when(mockBridgeHelper.getStudyIdsUsingSchedule(Exporter3TestUtil.APP_ID, SCHEDULE_GUID)).thenReturn(ImmutableList.of(
                Exporter3TestUtil.STUDY_ID,
                "non-participant-study"));
        when(mockBridgeHelper.getStudy(Exporter3TestUtil.APP_ID, Exporter3TestUtil.STUDY_ID)).thenReturn(Exporter3TestUtil.makeStudyWithEx3Config());

        Upload mockUpload = mockUpload(false);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(mockUpload);

        mockSynapseHelper();

        // Execute.
        processor.process(makeRequest());

        // Just verify that we only export for study STUDY_ID, and not non-timeline-study or non-participant-study/
        verify(mockS3Helper).copyS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), eq(RAW_DATA_BUCKET),
                eq(EXPECTED_S3_KEY_FOR_STUDY), any());
        verifyNoMoreInteractions(mockS3Helper);

        // Verify create file handle.
        ArgumentCaptor<S3FileHandle> fileHandleCaptor = ArgumentCaptor.forClass(S3FileHandle.class);
        verify(mockSynapseHelper, times(1)).createS3FileHandleWithRetry(fileHandleCaptor
                .capture());

        S3FileHandle fileHandle = fileHandleCaptor.getValue();
        assertEquals(fileHandle.getKey(), EXPECTED_S3_KEY_FOR_STUDY);
    }

    @Test
    public void appAndStudiesNotConfigured() throws Exception {
        // Mock services.
        App app = new App();
        app.setIdentifier(Exporter3TestUtil.APP_ID);
        app.setExporter3Enabled(true);
        app.setExporter3Configuration(null);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(app);

        StudyParticipant mockParticipant = mockParticipant();
        when(mockParticipant.getStudyIds()).thenReturn(ImmutableList.of("study1", "study2"));
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        Study study = new Study();
        study.setExporter3Enabled(false);
        when(mockBridgeHelper.getStudy(eq(Exporter3TestUtil.APP_ID), any())).thenReturn(study);

        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID)).thenReturn(makeRecord());

        // Execute.
        processor.process(makeRequest());

        // Verify calls to services.
        verify(mockSynapseHelper).isSynapseWritable();
        verify(mockBridgeHelper).getApp(Exporter3TestUtil.APP_ID);
        verify(mockBridgeHelper).getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID);
        verify(mockBridgeHelper).getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false);
        verify(mockBridgeHelper).getStudy(Exporter3TestUtil.APP_ID, "study1");
        verify(mockBridgeHelper).getStudy(Exporter3TestUtil.APP_ID, "study2");

        // Verify no more interactions with backend services.
        verifyNoMoreInteractions(mockBridgeHelper, mockSynapseHelper);
    }

    @Test
    public void nullContentType() throws Exception {
        Upload upload = mockUpload(false);
        when(upload.getContentType()).thenReturn(null);
        when(mockBridgeHelper.getUploadByUploadId(any())).thenReturn(upload);
        when(mockBridgeHelper.getApp(Exporter3TestUtil.APP_ID)).thenReturn(Exporter3TestUtil.makeAppWithEx3Config());
        FileEntity createdFileEntity = new FileEntity();
        createdFileEntity.setId(EXPORTED_FILE_ENTITY_ID);
        mockSynapseHelper();
        when(mockBridgeHelper.getHealthDataRecordForExporter3(Exporter3TestUtil.APP_ID, RECORD_ID))
                .thenReturn(makeRecord());
        StudyParticipant mockParticipant = mockParticipant();
        when(mockBridgeHelper.getParticipantByHealthCode(Exporter3TestUtil.APP_ID, HEALTH_CODE, false))
                .thenReturn(mockParticipant);

        processor.process(makeRequest());

        ArgumentCaptor<Map> annotationMapCaptor = ArgumentCaptor.forClass(Map.class);
        verify(mockSynapseHelper).addAnnotationsToEntity(eq(EXPORTED_FILE_ENTITY_ID), annotationMapCaptor.capture());
        Map<String, AnnotationsValue> annotationMap = annotationMapCaptor.getValue();
        assertFalse(annotationMap.containsKey(Exporter3WorkerProcessor.METADATA_KEY_CONTENT_TYPE));
    }

    private void mockSynapseHelper() throws Exception {
        // Mock create folder.
        when(mockSynapseHelper.createFolderIfNotExists(Exporter3TestUtil.RAW_FOLDER_ID, TODAYS_DATE_STRING))
                .thenReturn(TODAYS_FOLDER_ID);

        // Mock create file handle.
        S3FileHandle createdFileHandle = new S3FileHandle();
        createdFileHandle.setId(EXPORTED_FILE_HANDLE_ID);
        when(mockSynapseHelper.createS3FileHandleWithRetry(any())).thenReturn(createdFileHandle);

        // Mock create file entity.
        FileEntity createdFileEntity = new FileEntity();
        createdFileEntity.setId(EXPORTED_FILE_ENTITY_ID);
        when(mockSynapseHelper.createEntityWithRetry(any(FileEntity.class))).thenReturn(createdFileEntity);
    }

    private void verifyS3Metadata(ObjectMetadata s3Metadata) {
        assertEquals(s3Metadata.getSSEAlgorithm(), ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        assertEquals(s3Metadata.getContentType(), CONTENT_TYPE);

        Map<String, String> userMetadataMap = s3Metadata.getUserMetadata();
        assertEquals(userMetadataMap.size(), 9);
        assertEquals(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_CLIENT_INFO), CLIENT_INFO);
        assertEquals(DateTime.parse(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_EXPORTED_ON)).getMillis(),
                MOCK_NOW_MILLIS);
        assertEquals(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_HEALTH_CODE), HEALTH_CODE);
        assertEquals(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_PARTICIPANT_VERSION),
                String.valueOf(PARTICIPANT_VERSION));
        assertEquals(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_RECORD_ID), RECORD_ID);
        assertEquals(DateTime.parse(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_UPLOADED_ON)).getMillis(),
                UPLOADED_ON_MILLIS);
        assertEquals(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_USER_AGENT), USER_AGENT);
        assertEquals(userMetadataMap.get(CUSTOM_METADATA_KEY_SANITIZED), CUSTOM_METADATA_VALUE_CLEAN);
        assertEquals(userMetadataMap.get(Exporter3WorkerProcessor.METADATA_KEY_CONTENT_TYPE), CONTENT_TYPE);
    }

    private void verifySynapseExport(String expectedS3Key) throws Exception {
        // Verify create file handle.
        ArgumentCaptor<S3FileHandle> fileHandleCaptor = ArgumentCaptor.forClass(S3FileHandle.class);
        verify(mockSynapseHelper).createS3FileHandleWithRetry(fileHandleCaptor.capture());

        S3FileHandle fileHandle = fileHandleCaptor.getValue();
        assertEquals(fileHandle.getBucketName(), RAW_DATA_BUCKET);
        assertEquals(fileHandle.getContentType(), CONTENT_TYPE);
        assertEquals(fileHandle.getFileName(), FULL_FILENAME);
        assertEquals(fileHandle.getKey(), expectedS3Key);
        assertEquals(fileHandle.getStorageLocationId().longValue(), Exporter3TestUtil.STORAGE_LOCATION_ID);
        assertEquals(Hex.decode(fileHandle.getContentMd5()), DUMMY_MD5_BYTES);

        // Verify create file entity.
        ArgumentCaptor<FileEntity> fileEntityCaptor = ArgumentCaptor.forClass(FileEntity.class);
        verify(mockSynapseHelper).createEntityWithRetry(fileEntityCaptor.capture());

        FileEntity fileEntity = fileEntityCaptor.getValue();
        assertEquals(fileEntity.getDataFileHandleId(), EXPORTED_FILE_HANDLE_ID);
        assertEquals(fileEntity.getName(), FULL_FILENAME);
        assertEquals(fileEntity.getParentId(), TODAYS_FOLDER_ID);

        // Verify annotations.
        ArgumentCaptor<Map> annotationMapCaptor = ArgumentCaptor.forClass(Map.class);
        verify(mockSynapseHelper).addAnnotationsToEntity(eq(EXPORTED_FILE_ENTITY_ID), annotationMapCaptor.capture());

        Map<String, AnnotationsValue> annotationMap = annotationMapCaptor.getValue();
        assertEquals(annotationMap.size(), 9);

        // Verify that all annotations are of type string and have one value.
        Map<String, String> flattenedAnnotationMap = new HashMap<>();
        for (Map.Entry<String, AnnotationsValue> annotationEntry : annotationMap.entrySet()) {
            String name = annotationEntry.getKey();
            AnnotationsValue value = annotationEntry.getValue();
            if (Exporter3WorkerProcessor.METADATA_KEY_PARTICIPANT_VERSION.equals(name)) {
                // participantVersion is special. This needs to be joined with the ParticipantVersion table, so it's
                // a number.
                assertEquals(value.getType(), AnnotationsValueType.LONG);
            } else {
                assertEquals(value.getType(), AnnotationsValueType.STRING);
            }
            assertEquals(value.getValue().size(), 1);
            String valueString = value.getValue().get(0);
            flattenedAnnotationMap.put(name, valueString);
        }

        assertEquals(flattenedAnnotationMap.size(), 9);
        assertEquals(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_CLIENT_INFO), CLIENT_INFO);
        assertEquals(DateTime.parse(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_EXPORTED_ON)).getMillis(),
                MOCK_NOW_MILLIS);
        assertEquals(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_HEALTH_CODE), HEALTH_CODE);
        assertEquals(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_PARTICIPANT_VERSION),
                String.valueOf(PARTICIPANT_VERSION));
        assertEquals(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_RECORD_ID), RECORD_ID);
        assertEquals(DateTime.parse(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_UPLOADED_ON)).getMillis(),
                UPLOADED_ON_MILLIS);
        assertEquals(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_USER_AGENT), USER_AGENT);
        assertEquals(flattenedAnnotationMap.get(CUSTOM_METADATA_KEY_SANITIZED), CUSTOM_METADATA_VALUE_CLEAN);
        assertEquals(flattenedAnnotationMap.get(Exporter3WorkerProcessor.METADATA_KEY_CONTENT_TYPE), CONTENT_TYPE);
    }

    private void verifyUpdatedRecordForApp() throws Exception {
        HealthDataRecordEx3 record = verifyUpdatedRecordInternal();

        ExportedRecordInfo exportedRecord = record.getExportedRecord();
        assertNotNull(exportedRecord);
        verifyNotificationRecordInfo(exportedRecord, EXPECTED_S3_KEY);
    }

    private void verifyUpdatedRecordForStudy() throws Exception {
        HealthDataRecordEx3 record = verifyUpdatedRecordInternal();

        ExportedRecordInfo exportedRecord = record.getExportedStudyRecords().get(Exporter3TestUtil.STUDY_ID);
        assertNotNull(exportedRecord);
        verifyNotificationRecordInfo(exportedRecord, EXPECTED_S3_KEY_FOR_STUDY);
    }

    private HealthDataRecordEx3 verifyUpdatedRecordInternal() throws Exception {
        ArgumentCaptor<HealthDataRecordEx3> recordCaptor = ArgumentCaptor.forClass(HealthDataRecordEx3.class);
        verify(mockBridgeHelper).createOrUpdateHealthDataRecordForExporter3(eq(Exporter3TestUtil.APP_ID), recordCaptor.capture());

        HealthDataRecordEx3 record = recordCaptor.getValue();
        assertEquals(record.getExportedOn().getMillis(), MOCK_NOW_MILLIS);
        assertTrue(record.isExported());
        return record;
    }

    private void verifyExportNotificationForApp() throws Exception {
        ArgumentCaptor<ExportToAppNotification> notificationCaptor = ArgumentCaptor.forClass(
                ExportToAppNotification.class);
        verify(mockBridgeHelper).sendExportNotifications(notificationCaptor.capture());
        ExportToAppNotification notification = notificationCaptor.getValue();
        assertEquals(notification.getAppId(), Exporter3TestUtil.APP_ID);
        assertEquals(notification.getRecordId(), RECORD_ID);
        verifyNotificationRecordInfo(notification.getRecord(), EXPECTED_S3_KEY);
        assertTrue(notification.getStudyRecords().isEmpty());
    }

    private void verifyExportNotificationForStudy() throws Exception {
        ArgumentCaptor<ExportToAppNotification> notificationCaptor = ArgumentCaptor.forClass(
                ExportToAppNotification.class);
        verify(mockBridgeHelper).sendExportNotifications(notificationCaptor.capture());
        ExportToAppNotification notification = notificationCaptor.getValue();
        assertEquals(notification.getAppId(), Exporter3TestUtil.APP_ID);
        assertEquals(notification.getRecordId(), RECORD_ID);
        assertNull(notification.getRecord());
        assertEquals(notification.getStudyRecords().size(), 1);
        verifyNotificationRecordInfo(notification.getStudyRecords().get(Exporter3TestUtil.STUDY_ID),
                EXPECTED_S3_KEY_FOR_STUDY);
    }

    private void verifyNotificationRecordInfo(ExportedRecordInfo recordInfo, String expectedS3Key) {
        assertEquals(recordInfo.getParentProjectId(), Exporter3TestUtil.PROJECT_ID);
        assertEquals(recordInfo.getRawFolderId(), TODAYS_FOLDER_ID);
        assertEquals(recordInfo.getFileEntityId(), EXPORTED_FILE_ENTITY_ID);
        assertEquals(recordInfo.getS3Bucket(), RAW_DATA_BUCKET);
        assertEquals(recordInfo.getS3Key(), expectedS3Key);
    }

    private static Exporter3Request makeRequest() {
        Exporter3Request request = new Exporter3Request();
        request.setAppId(Exporter3TestUtil.APP_ID);
        request.setRecordId(RECORD_ID);
        return request;
    }

    private static HealthDataRecordEx3 makeRecord() {
        HealthDataRecordEx3 record = new HealthDataRecordEx3();
        record.setAppId(Exporter3TestUtil.APP_ID);
        record.setId(RECORD_ID);

        record.setClientInfo(CLIENT_INFO);
        record.setHealthCode(HEALTH_CODE);
        record.setCreatedOn(UPLOADED_ON);
        record.setParticipantVersion(PARTICIPANT_VERSION);
        record.setSharingScope(SharingScope.ALL_QUALIFIED_RESEARCHERS);
        record.setUserAgent(USER_AGENT);

        record.putMetadataItem(CUSTOM_METADATA_KEY, CUSTOM_METADATA_VALUE);

        // Add a fake client info, just to make sure Bridge overwrites/ignores this correctly.
        record.putMetadataItem(Exporter3WorkerProcessor.METADATA_KEY_CLIENT_INFO, "this is ignored");
        record.putMetadataItem(Exporter3WorkerProcessor.METADATA_KEY_USER_AGENT, "this is also ignored");

        return record;
    }

    private static Upload mockUpload(boolean encrypted) {
        Upload upload = mock(Upload.class);
        when(upload.getUploadId()).thenReturn(RECORD_ID);
        when(upload.getRecordId()).thenReturn(RECORD_ID);

        when(upload.getCompletedOn()).thenReturn(UPLOADED_ON);
        when(upload.getContentType()).thenReturn(CONTENT_TYPE);
        when(upload.getContentMd5()).thenReturn(Base64.getEncoder().encodeToString(DUMMY_MD5_BYTES));
        when(upload.isEncrypted()).thenReturn(encrypted);
        when(upload.getFilename()).thenReturn(FILENAME);
        return upload;
    }

    private static StudyParticipant mockParticipant() {
        StudyParticipant participant = mock(StudyParticipant.class);
        when(participant.getSharingScope()).thenReturn(SharingScope.ALL_QUALIFIED_RESEARCHERS);

        // This isn't realistic, but for test purposes, let's use an empty list for study IDs.
        when(participant.getStudyIds()).thenReturn(ImmutableList.of());

        return participant;
    }
}
