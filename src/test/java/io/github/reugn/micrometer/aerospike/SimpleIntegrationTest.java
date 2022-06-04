package io.github.reugn.micrometer.aerospike;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import javax.annotation.Nonnull;
import java.time.Duration;

public class SimpleIntegrationTest {

    private static final MeterRegistry registry;
    private static final Clock clock;

    static {
        final AerospikeConfig config = new AerospikeConfig() {
            @Override
            public String get(@Nonnull String key) {
                return null;
            }

            @Override
            @Nonnull
            public Duration step() {
                return Duration.ofSeconds(1);
            }
        };
        clock = Clock.SYSTEM;
        registry = new AerospikeMeterRegistry(config, clock);
    }

    public static void main(String[] args) {
        final Counter counter = Counter
                .builder("call.counter")
                .tag("type", "test")
                .register(registry);
        final Timer timer = Timer
                .builder("call.timer")
                .register(registry);

        for (int i = 0; i < 1000; i++) {
            timer.record(() -> call(counter));
        }
    }

    private static void call(Counter counter) {
        counter.increment();
        try {
            Thread.sleep(10);
        } catch (InterruptedException ignore) {
        }
    }
}
