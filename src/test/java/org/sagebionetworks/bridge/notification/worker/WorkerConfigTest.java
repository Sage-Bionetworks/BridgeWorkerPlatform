package org.sagebionetworks.bridge.notification.worker;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

public class WorkerConfigTest {
    private static final Map<String, String> DUMMY_MAP = ImmutableMap.of("dummy key", "dummy value");
    private static final Set<String> DUMMY_SET = ImmutableSet.of("dummy string");

    @Test
    public void burstStartEventIdSetNeverNull() {
        testSetNeverNull(WorkerConfig::getBurstStartEventIdSet, WorkerConfig::setBurstStartEventIdSet);
    }

    @Test
    public void excludedDataGroupSetNeverNull() {
        testSetNeverNull(WorkerConfig::getExcludedDataGroupSet, WorkerConfig::setExcludedDataGroupSet);
    }

    @Test
    public void missedCumulativeActivitiesMessagesByDataGroupNeverNull() {
        testMapNeverNull(WorkerConfig::getMissedCumulativeActivitiesMessagesByDataGroup,
                WorkerConfig::setMissedCumulativeActivitiesMessagesByDataGroup);
    }

    @Test
    public void missedEarlyActivitiesMessagesByDataGroupNeverNull() {
        testMapNeverNull(WorkerConfig::getMissedEarlyActivitiesMessagesByDataGroup,
                WorkerConfig::setMissedEarlyActivitiesMessagesByDataGroup);
    }

    @Test
    public void missedLaterActivitiesMessagesByDataGroupNeverNull() {
        testMapNeverNull(WorkerConfig::getMissedLaterActivitiesMessagesByDataGroup,
                WorkerConfig::setMissedLaterActivitiesMessagesByDataGroup);
    }

    @Test
    public void preburstMessagesByDataGroupNeverNull() {
        testMapNeverNull(WorkerConfig::getPreburstMessagesByDataGroup, WorkerConfig::setPreburstMessagesByDataGroup);
    }

    @Test
    public void requiredDataGroupsOneOfSetNeverNull() {
        testSetNeverNull(WorkerConfig::getRequiredDataGroupsOneOfSet, WorkerConfig::setRequiredDataGroupsOneOfSet);
    }

    @Test
    public void requiredSubpopulationGuidSetNeverNull() {
        testSetNeverNull(WorkerConfig::getRequiredSubpopulationGuidSet, WorkerConfig::setRequiredSubpopulationGuidSet);
    }

    private static void testSetNeverNull(Function<WorkerConfig, Set<String>> getter,
            BiConsumer<WorkerConfig, Set<String>> setter) {
        // Initially empty
        WorkerConfig config = new WorkerConfig();
        assertTrue(getter.apply(config).isEmpty());

        // Set works
        setter.accept(config, DUMMY_SET);
        assertEquals(getter.apply(config), DUMMY_SET);

        // Set to null gives us an empty set
        setter.accept(config, null);
        assertTrue(getter.apply(config).isEmpty());
    }

    // Set and Map don't have a shared parent class, so sadly, we'll need to duplicate the test twice.
    private static void testMapNeverNull(Function<WorkerConfig, Map<String, String>> getter,
            BiConsumer<WorkerConfig, Map<String, String>> setter) {
        // Initially empty
        WorkerConfig config = new WorkerConfig();
        assertTrue(getter.apply(config).isEmpty());

        // Set works
        setter.accept(config, DUMMY_MAP);
        assertEquals(getter.apply(config), DUMMY_MAP);

        // Set to null gives us an empty set
        setter.accept(config, null);
        assertTrue(getter.apply(config).isEmpty());
    }
}
