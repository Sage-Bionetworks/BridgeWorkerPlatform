package org.sagebionetworks.bridge.udd.retry;

import static org.testng.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import com.jcabi.aspects.RetryOnFailure;
import org.testng.annotations.Test;

// Basic test to test we properly integrated retry library.
public class RetryTest {
    private static class RetryTestHelper {
        int timesCalled = 0;

        @RetryOnFailure(attempts = 5, delay = 10, unit = TimeUnit.MILLISECONDS, randomize = false)
        void call() {
            if (++timesCalled < 5) {
                throw new IllegalStateException();
            }
        }
    }

    @Test
    public void testRetry() {
        RetryTestHelper helper = new RetryTestHelper();
        helper.call();
        assertEquals(helper.timesCalled, 5);
    }
}
