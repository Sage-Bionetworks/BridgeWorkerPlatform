package org.sagebionetworks.bridge.udd.synapse;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
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
    private static final UploadSchemaKey TEST_SCHEMA_KEY = new UploadSchemaKey.Builder().withStudyId("test-study")
            .withSchemaId("test-schema").withRevision(42).build();
    private static final UploadSchema TEST_SCHEMA = new UploadSchema.Builder().withKey(TEST_SCHEMA_KEY)
            .addField("foo", "STRING").build();

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*synapseTableId.*")
    public void nullSynapseTableId() {
        new SynapseDownloadFromTableParameters.Builder().withHealthCode("test-health-code")
                .withStartDate(TEST_START_DATE).withEndDate(TEST_END_DATE).withTempDir(DUMMY_FILE)
                .withSchema(TEST_SCHEMA).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*synapseTableId.*")
    public void emptySynapseTableId() {
        new SynapseDownloadFromTableParameters.Builder().withSynapseTableId("").withHealthCode("test-health-code")
                .withStartDate(TEST_START_DATE).withEndDate(TEST_END_DATE).withTempDir(DUMMY_FILE)
                .withSchema(TEST_SCHEMA).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*healthCode.*")
    public void nullHealthCode() {
        new SynapseDownloadFromTableParameters.Builder().withSynapseTableId("test-table-id")
                .withStartDate(TEST_START_DATE).withEndDate(TEST_END_DATE).withTempDir(DUMMY_FILE)
                .withSchema(TEST_SCHEMA).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*healthCode.*")
    public void emptyHealthCode() {
        new SynapseDownloadFromTableParameters.Builder().withSynapseTableId("test-table-id").withHealthCode("")
                .withStartDate(TEST_START_DATE).withEndDate(TEST_END_DATE).withTempDir(DUMMY_FILE)
                .withSchema(TEST_SCHEMA).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*startDate.*")
    public void nullStartDate() {
        new SynapseDownloadFromTableParameters.Builder().withSynapseTableId("test-table-id")
                .withHealthCode("test-health-code").withEndDate(TEST_END_DATE).withTempDir(DUMMY_FILE)
                .withSchema(TEST_SCHEMA).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*endDate.*")
    public void nullEndDate() {
        new SynapseDownloadFromTableParameters.Builder().withSynapseTableId("test-table-id")
                .withHealthCode("test-health-code").withStartDate(TEST_START_DATE).withTempDir(DUMMY_FILE)
                .withSchema(TEST_SCHEMA).build();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void startDateAfterEndDate() {
        new SynapseDownloadFromTableParameters.Builder().withSynapseTableId("test-table-id")
                .withHealthCode("test-health-code").withStartDate(LocalDate.parse("2015-09-16"))
                .withEndDate(LocalDate.parse("2015-09-01")).withTempDir(DUMMY_FILE).withSchema(TEST_SCHEMA)
                .build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*tempDir.*")
    public void nullTempDir() {
        new SynapseDownloadFromTableParameters.Builder().withSynapseTableId("test-table-id")
                .withHealthCode("test-health-code").withStartDate(TEST_START_DATE).withEndDate(TEST_END_DATE)
                .withSchema(TEST_SCHEMA).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*schema.*")
    public void nullSchema() {
        new SynapseDownloadFromTableParameters.Builder().withSynapseTableId("test-table-id")
                .withHealthCode("test-health-code").withStartDate(TEST_START_DATE).withEndDate(TEST_END_DATE)
                .withTempDir(DUMMY_FILE).build();
    }

    @Test
    public void startDateBeforeEndDate() {
        SynapseDownloadFromTableParameters param = new SynapseDownloadFromTableParameters.Builder()
                .withSynapseTableId("test-table-id").withHealthCode("test-health-code").withStartDate(TEST_START_DATE)
                .withEndDate(TEST_END_DATE).withTempDir(DUMMY_FILE).withSchema(TEST_SCHEMA).build();

        assertEquals(param.getSynapseTableId(), "test-table-id");
        assertEquals(param.getHealthCode(), "test-health-code");
        assertEquals(param.getStartDate().toString(), "2015-03-09");
        assertEquals(param.getEndDate().toString(), "2015-09-16");
        assertSame(param.getTempDir(), DUMMY_FILE);
        assertEquals(param.getSchema().getKey().toString(), "test-study-test-schema-v42");
    }

    @Test
    public void startDateEqualsEndDate() {
        SynapseDownloadFromTableParameters param = new SynapseDownloadFromTableParameters.Builder()
                .withSynapseTableId("test-table-id").withHealthCode("test-health-code")
                .withStartDate(LocalDate.parse("2015-09-07")).withEndDate(LocalDate.parse("2015-09-07"))
                .withTempDir(DUMMY_FILE).withSchema(TEST_SCHEMA).build();

        // Just test the start and end date. The other params have already been tested.
        assertEquals(param.getStartDate().toString(), "2015-09-07");
        assertEquals(param.getEndDate().toString(), "2015-09-07");
    }
}
