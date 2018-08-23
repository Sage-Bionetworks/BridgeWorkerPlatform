package org.sagebionetworks.bridge.notification.worker;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

public class WorkerConfigTest {
    private static final List<String> DUMMY_LIST = ImmutableList.of("dummy string");
    private static final Map<String, List<String>> DUMMY_MAP = ImmutableMap.of("dummy key", ImmutableList.of(
            "dummy value"));
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
    public void missedCumulativeActivitiesMessagesListNeverNull() {
        testListNeverNull(WorkerConfig::getMissedCumulativeActivitiesMessagesList,
                WorkerConfig::setMissedCumulativeActivitiesMessagesList);
    }

    @Test
    public void missedEarlyActivitiesMessagesListNeverNull() {
        testListNeverNull(WorkerConfig::getMissedEarlyActivitiesMessagesList,
                WorkerConfig::setMissedEarlyActivitiesMessagesList);
    }

    @Test
    public void missedLaterActivitiesMessagesListNeverNull() {
        testListNeverNull(WorkerConfig::getMissedLaterActivitiesMessagesList,
                WorkerConfig::setMissedLaterActivitiesMessagesList);
    }

    @Test
    public void preburstMessagesByDataGroupNeverNull() {
        testMapNeverNull(WorkerConfig::getPreburstMessagesByDataGroup, WorkerConfig::setPreburstMessagesByDataGroup);
    }

    // The following tests are duplicated per collection type, because generics and inheritance behaves in ways that
    // make this difficult to genericize.

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

    private static void testListNeverNull(Function<WorkerConfig, List<String>> getter,
            BiConsumer<WorkerConfig, List<String>> setter) {
        // Initially empty
        WorkerConfig config = new WorkerConfig();
        assertTrue(getter.apply(config).isEmpty());

        // Set works
        setter.accept(config, DUMMY_LIST);
        assertEquals(getter.apply(config), DUMMY_LIST);

        // Set to null gives us an empty set
        setter.accept(config, null);
        assertTrue(getter.apply(config).isEmpty());
    }

    // Set and Map don't have a shared parent class, so sadly, we'll need to duplicate the test twice.
    private static void testMapNeverNull(Function<WorkerConfig, Map<String, List<String>>> getter,
            BiConsumer<WorkerConfig, Map<String, List<String>>> setter) {
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
