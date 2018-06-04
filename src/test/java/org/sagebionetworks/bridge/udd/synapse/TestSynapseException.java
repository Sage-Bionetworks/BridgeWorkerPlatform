package org.sagebionetworks.bridge.udd.synapse;

import org.sagebionetworks.client.exceptions.SynapseException;

// Dummy exception, because the real SynapseException is abstract, and there's no simple alternative to use for
// tests.
@SuppressWarnings("serial")
public class TestSynapseException extends SynapseException {
    public TestSynapseException() {
    }

    public TestSynapseException(String message, Throwable cause) {
        super(message, cause);
    }

    public TestSynapseException(String message) {
        super(message);
    }

    public TestSynapseException(Throwable cause) {
        super(cause);
    }
}
