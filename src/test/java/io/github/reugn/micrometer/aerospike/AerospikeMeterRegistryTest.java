package io.github.reugn.micrometer.aerospike;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class AerospikeMeterRegistryTest {

    private final AerospikeConfig config = AerospikeConfig.DEFAULT;

    private final MockClock clock = new MockClock();

    private final AerospikeMeterRegistry meterRegistry = new AerospikeMeterRegistry(config, clock);

    @Test
    void handleGauge() {
        meterRegistry.gauge("gauge", 1d);
        Gauge gauge = meterRegistry.find("gauge").gauge();
        assertNotNull(gauge);
        assertTrue(meterRegistry.handleGauge(gauge).isPresent());
    }

    @Test
    void handleInvalidGauge() {
        meterRegistry.gauge("gauge", Double.NaN);
        Gauge gauge = meterRegistry.find("gauge").gauge();
        assertNotNull(gauge);
        assertFalse(meterRegistry.handleGauge(gauge).isPresent());
    }

    @Test
    void handleCounter() {
        meterRegistry.counter("counter", "tag1", "val1");
        Counter counter = meterRegistry.find("counter").counter();
        assertNotNull(counter);
        assertTrue(meterRegistry.handleCounter(counter).isPresent());
    }

    @Test
    void handleTimer() {
        meterRegistry.timer("timer", "tag1", "val1");
        Timer timer = meterRegistry.find("timer").timer();
        assertNotNull(timer);
        assertTrue(meterRegistry.handleTimer(timer).isPresent());
    }
}
