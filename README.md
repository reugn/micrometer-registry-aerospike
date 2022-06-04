# micrometer-registry-aerospike

## Library tools
* A [Micrometer](https://micrometer.io/) registry implementation for sending metrics to [Aerospike](https://aerospike.com/).
* A metric-aware [Aerospike Java Client](https://github.com/aerospike/aerospike-client-java) to collect miscellaneous activity information using one of the Micrometer registries.

## Examples
* Create an Aerospike Micrometer registry. Use `AerospikeConfig` for configuration.
```java
AerospikeConfig config = new AerospikeConfig() {
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
MeterRegistry registry = new AerospikeMeterRegistry(config, Clock.SYSTEM);
```

* Create a metric aware Aerospike client. See the previous example for the `config` and `registry` variables.
```java
IAerospikeClient client = new AerospikeClientMicrometer(
    new AerospikeClient(config.clientPolicy(), config.hosts()),
    registry
);
```

More examples can be found in the tests section.
