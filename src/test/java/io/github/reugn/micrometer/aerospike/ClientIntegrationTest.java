package io.github.reugn.micrometer.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import io.github.reugn.micrometer.aerospike.client.AerospikeClientMicrometer;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientIntegrationTest {

    private static final String dataSetName = "test";

    private static final AerospikeConfig config;
    private static final MeterRegistry registry;
    private static final Clock clock;

    static {
        config = new AerospikeConfig() {
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
        IAerospikeClient client = new AerospikeClientMicrometer(
                new AerospikeClient(config.clientPolicy(), config.hosts()),
                registry
        );

        final AtomicBoolean active = new AtomicBoolean(true);
        final Queue<Key> keys = new ConcurrentLinkedQueue<>();

        new Thread(() -> {
            while (active.get()) {
                Key key = getKey();
                keys.offer(key);
                client.put(null, key, new Bin("int", 1));
                try {
                    Thread.sleep(10);
                } catch (Exception ignore) {
                }
            }
        }).start();

        new Thread(() -> {
            while (active.get()) {
                Key key = keys.poll();
                if (key != null) {
                    client.delete(null, key);
                }
                try {
                    Thread.sleep(10);
                } catch (Exception ignore) {
                }
            }
        }).start();

        new Thread(() -> {
            try {
                Thread.sleep(10000);
            } catch (Exception ignore) {
            }
            active.set(false);
            client.truncate(null, config.namespace(), dataSetName, null);
            try {
                Thread.sleep(1000);
            } catch (Exception ignore) {
            }
        }).start();
    }

    private static Key getKey() {
        return new Key(config.namespace(), dataSetName, UUID.randomUUID().toString());
    }
}
