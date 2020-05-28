package org.sagebionetworks.bridge.udd.synapse;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import java.io.File;

import org.joda.time.LocalDate;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.schema.UploadSchemaKey;

public class SynapseDownloadFromTableParametersTest {
    private static final File DUMMY_FILE = mock(File.class);
    private static final LocalDate TEST_START_DATE = LocalDate.parse("2015-03-09");
    private static final LocalDate TEST_END_DATE = LocalDate.parse("2015-09-16");
    private static final String TEST_HEALTH_CODE = "test-health-code";
    private static final String TEST_TABLE_ID = "test-table";

    private static final String TEST_APP_ID = "test-app";
    private static final UploadSchemaKey TEST_SCHEMA_KEY = new UploadSchemaKey.Builder().withAppId(TEST_APP_ID)
            .withSchemaId("test-schema").withRevision(42).build();
    private static final UploadSchema TEST_SCHEMA = new UploadSchema.Builder().withKey(TEST_SCHEMA_KEY)
            .addField("foo", "STRING").build();

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*synapseTableId.*")
    public void nullSynapseTableId() {
        makeValidParamBuilder().withSynapseTableId(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*synapseTableId.*")
    public void emptySynapseTableId() {
        makeValidParamBuilder().withSynapseTableId("").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*synapseTableId.*")
    public void blankSynapseTableId() {
        makeValidParamBuilder().withSynapseTableId("   ").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*healthCode.*")
    public void nullHealthCode() {
        makeValidParamBuilder().withHealthCode(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*healthCode.*")
    public void emptyHealthCode() {
        makeValidParamBuilder().withHealthCode("").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*healthCode.*")
    public void blankHealthCode() {
        makeValidParamBuilder().withHealthCode("   ").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*startDate.*")
    public void nullStartDate() {
        makeValidParamBuilder().withStartDate(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*endDate.*")
    public void nullEndDate() {
        makeValidParamBuilder().withEndDate(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void startDateAfterEndDate() {
        makeValidParamBuilder().withStartDate(TEST_END_DATE).withEndDate(TEST_START_DATE).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*tempDir.*")
    public void nullTempDir() {
        makeValidParamBuilder().withTempDir(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*appId.*")
    public void nullAppId() {
        makeValidParamBuilder().withAppId(null).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*appId.*")
    public void emptyAppId() {
        makeValidParamBuilder().withAppId("").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*appId.*")
    public void blankAppId() {
        makeValidParamBuilder().withAppId("   ").build();
    }

    @Test
    public void normalCase() {
        SynapseDownloadFromTableParameters param = makeValidParamBuilder().build();

        assertEquals(param.getSynapseTableId(), TEST_TABLE_ID);
        assertEquals(param.getHealthCode(), TEST_HEALTH_CODE);
        assertEquals(param.getStartDate(), TEST_START_DATE);
        assertEquals(param.getEndDate(), TEST_END_DATE);
        assertSame(param.getTempDir(), DUMMY_FILE);
        assertNull(param.getSchema());
        assertEquals(param.getAppId(), TEST_APP_ID);
    }

    @Test
    public void startDateEqualsEndDate() {
        SynapseDownloadFromTableParameters param = makeValidParamBuilder().withStartDate(TEST_END_DATE)
                .withEndDate(TEST_END_DATE).build();

        // Just test the start and end date. The other params have already been tested.
        assertEquals(param.getStartDate(), TEST_END_DATE);
        assertEquals(param.getEndDate(), TEST_END_DATE);
    }

    @Test
    public void optionalSchema() {
        SynapseDownloadFromTableParameters param = makeValidParamBuilder().withSchema(TEST_SCHEMA).build();

        // Just test the schema. The other params have already been tested.
        assertSame(param.getSchema(), TEST_SCHEMA);
    }

    private static SynapseDownloadFromTableParameters.Builder makeValidParamBuilder() {
        return new SynapseDownloadFromTableParameters.Builder()
                .withSynapseTableId(TEST_TABLE_ID).withHealthCode(TEST_HEALTH_CODE).withStartDate(TEST_START_DATE)
                .withEndDate(TEST_END_DATE).withTempDir(DUMMY_FILE).withAppId(TEST_APP_ID);
    }
}
