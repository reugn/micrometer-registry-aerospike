package io.github.reugn.micrometer.aerospike;

import com.aerospike.client.Host;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.ClientPolicy;
import io.micrometer.core.instrument.step.StepRegistryConfig;

import javax.annotation.Nonnull;

public interface AerospikeConfig extends StepRegistryConfig {

    /**
     * Accept configuration defaults
     */
    AerospikeConfig DEFAULT = k -> null;

    default ClientPolicy clientPolicy() {
        return new ClientPolicy();
    }

    default BatchWritePolicy batchWritePolicy() {
        return new BatchWritePolicy();
    }

    default BatchPolicy batchPolicy() {
        return new BatchPolicy();
    }

    default Host[] hosts() {
        return new Host[]{new Host("localhost", 3000)};
    }

    default String namespace() {
        return "test";
    }

    default String setName() {
        return "micrometer";
    }

    /**
     * Property prefix to prepend to configuration names.
     *
     * @return property prefix
     */
    @Nonnull
    default String prefix() {
        return "aerospike";
    }
}
