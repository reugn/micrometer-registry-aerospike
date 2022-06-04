package io.github.reugn.micrometer.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.util.MeterPartition;
import io.micrometer.core.instrument.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AerospikeMeterRegistry extends StepMeterRegistry {

    private static final ThreadFactory DEFAULT_THREAD_FACTORY = new NamedThreadFactory("aerospike-metrics-publisher");

    private static final Logger logger = LoggerFactory.getLogger(AerospikeMeterRegistry.class);

    private final AerospikeConfig config;
    private final AerospikeClient client;

    public AerospikeMeterRegistry(AerospikeConfig config, Clock clock) {
        this(config, clock, DEFAULT_THREAD_FACTORY);
    }

    public AerospikeMeterRegistry(AerospikeConfig config, Clock clock, ThreadFactory threadFactory) {
        super(config, clock);

        config().namingConvention(new AerospikeNamingConvention());

        this.config = config;
        this.client = new AerospikeClient(config.clientPolicy(), config.hosts());
        start(threadFactory);
    }

    @Override
    protected void publish() {
        logger.debug(getClass().getSimpleName() + " publish");
        for (List<Meter> batch : MeterPartition.partition(this, config.batchSize())) {
            try {
                List<BatchRecord> batchRecords = batch.stream().map(m -> m.match(
                        this::handleGauge,
                        this::handleCounter,
                        this::handleTimer,
                        this::handleSummary,
                        this::handleLongTaskTimer,
                        this::handleTimeGauge,
                        this::handleFunctionCounter,
                        this::handleFunctionTimer,
                        this::handleCustomMetric)
                ).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
                if (!client.operate(config.batchPolicy(), batchRecords)) {
                    logger.warn("Failed to write some metrics to Aerospike");
                }
            } catch (Throwable t) {
                logger.warn("Failed to write metrics to Aerospike", t);
            }
        }
    }

    Optional<BatchRecord> handleGauge(Gauge gauge) {
        double value = gauge.value();
        if (!Double.isFinite(value)) return Optional.empty();
        long wallTime = config().clock().wallTime();
        String name = getConventionName(gauge.getId());
        List<Operation> ops = new ArrayList<>();
        ops.add(Operation.put(new Bin("type", "Gauge")));
        ops.add(Operation.put(new Bin("ts", wallTime)));
        ops.add(Operation.put(new Bin("name", name)));
        ops.add(Operation.put(new Bin("value", value)));
        addTagOps(gauge.getId(), ops);

        return Optional.of(new BatchWrite(
                config.batchWritePolicy(),
                getKey(name, wallTime),
                ops.toArray(new Operation[0])
        ));
    }

    Optional<BatchRecord> handleCounter(Counter counter) {
        long wallTime = config().clock().wallTime();
        String name = getConventionName(counter.getId());
        List<Operation> ops = new ArrayList<>();
        ops.add(Operation.put(new Bin("type", "Counter")));
        ops.add(Operation.put(new Bin("ts", wallTime)));
        ops.add(Operation.put(new Bin("name", name)));
        ops.add(Operation.put(new Bin("count", counter.count())));
        addTagOps(counter.getId(), ops);

        return Optional.of(new BatchWrite(
                config.batchWritePolicy(),
                getKey(name, wallTime),
                ops.toArray(new Operation[0])
        ));
    }

    Optional<BatchRecord> handleTimer(Timer timer) {
        long wallTime = config().clock().wallTime();
        String name = getConventionName(timer.getId());
        List<Operation> ops = new ArrayList<>();
        ops.add(Operation.put(new Bin("type", "Timer")));
        ops.add(Operation.put(new Bin("ts", wallTime)));
        ops.add(Operation.put(new Bin("name", name)));
        ops.add(Operation.put(new Bin("count", timer.count())));
        ops.add(Operation.put(new Bin("max", timer.max(getBaseTimeUnit()))));
        ops.add(Operation.put(new Bin("avg", timer.mean(getBaseTimeUnit()))));
        ops.add(Operation.put(new Bin("sum", timer.totalTime(getBaseTimeUnit()))));
        addTagOps(timer.getId(), ops);

        return Optional.of(new BatchWrite(
                config.batchWritePolicy(),
                getKey(name, wallTime),
                ops.toArray(new Operation[0])
        ));
    }

    Optional<BatchRecord> handleSummary(DistributionSummary summary) {
        long wallTime = config().clock().wallTime();
        String name = getConventionName(summary.getId());
        List<Operation> ops = new ArrayList<>();
        ops.add(Operation.put(new Bin("type", "DistributionSummary")));
        ops.add(Operation.put(new Bin("ts", wallTime)));
        ops.add(Operation.put(new Bin("name", name)));
        ops.add(Operation.put(new Bin("count", summary.count())));
        ops.add(Operation.put(new Bin("max", summary.max())));
        ops.add(Operation.put(new Bin("avg", summary.mean())));
        ops.add(Operation.put(new Bin("sum", summary.totalAmount())));
        addTagOps(summary.getId(), ops);

        return Optional.of(new BatchWrite(
                config.batchWritePolicy(),
                getKey(name, wallTime),
                ops.toArray(new Operation[0])
        ));
    }

    Optional<BatchRecord> handleLongTaskTimer(LongTaskTimer timer) {
        long wallTime = config().clock().wallTime();
        String name = getConventionName(timer.getId());
        List<Operation> ops = new ArrayList<>();
        ops.add(Operation.put(new Bin("type", "LongTaskTimer")));
        ops.add(Operation.put(new Bin("ts", wallTime)));
        ops.add(Operation.put(new Bin("name", name)));
        ops.add(Operation.put(new Bin("activeTasks", timer.activeTasks())));
        ops.add(Operation.put(new Bin("duration", timer.duration(getBaseTimeUnit()))));
        addTagOps(timer.getId(), ops);

        return Optional.of(new BatchWrite(
                config.batchWritePolicy(),
                getKey(name, wallTime),
                ops.toArray(new Operation[0])
        ));
    }

    Optional<BatchRecord> handleTimeGauge(TimeGauge timeGauge) {
        double value = timeGauge.value(getBaseTimeUnit());
        if (!Double.isFinite(value)) return Optional.empty();
        long wallTime = config().clock().wallTime();
        String name = getConventionName(timeGauge.getId());
        List<Operation> ops = new ArrayList<>();
        ops.add(Operation.put(new Bin("type", "TimeGauge")));
        ops.add(Operation.put(new Bin("ts", wallTime)));
        ops.add(Operation.put(new Bin("name", name)));
        ops.add(Operation.put(new Bin("value", value)));
        addTagOps(timeGauge.getId(), ops);

        return Optional.of(new BatchWrite(
                config.batchWritePolicy(),
                getKey(name, wallTime),
                ops.toArray(new Operation[0])
        ));
    }

    Optional<BatchRecord> handleFunctionCounter(FunctionCounter counter) {
        double count = counter.count();
        if (!Double.isFinite(count)) return Optional.empty();
        long wallTime = config().clock().wallTime();
        String name = getConventionName(counter.getId());
        List<Operation> ops = new ArrayList<>();
        ops.add(Operation.put(new Bin("type", "FunctionCounter")));
        ops.add(Operation.put(new Bin("ts", wallTime)));
        ops.add(Operation.put(new Bin("name", name)));
        ops.add(Operation.put(new Bin("count", count)));
        addTagOps(counter.getId(), ops);

        return Optional.of(new BatchWrite(
                config.batchWritePolicy(),
                getKey(name, wallTime),
                ops.toArray(new Operation[0])
        ));
    }

    Optional<BatchRecord> handleFunctionTimer(FunctionTimer timer) {
        long wallTime = config().clock().wallTime();
        String name = getConventionName(timer.getId());
        List<Operation> ops = new ArrayList<>();
        ops.add(Operation.put(new Bin("type", "FunctionTimer")));
        ops.add(Operation.put(new Bin("ts", wallTime)));
        ops.add(Operation.put(new Bin("name", name)));
        ops.add(Operation.put(new Bin("count", timer.count())));
        ops.add(Operation.put(new Bin("avg", timer.mean(getBaseTimeUnit()))));
        ops.add(Operation.put(new Bin("sum", timer.totalTime(getBaseTimeUnit()))));
        addTagOps(timer.getId(), ops);

        return Optional.of(new BatchWrite(
                config.batchWritePolicy(),
                getKey(name, wallTime),
                ops.toArray(new Operation[0])
        ));
    }

    Optional<BatchRecord> handleCustomMetric(Meter meter) {
        long wallTime = config().clock().wallTime();
        String name = getConventionName(meter.getId());
        List<Operation> ops = new ArrayList<>();
        ops.add(Operation.put(new Bin("type", "Custom")));
        ops.add(Operation.put(new Bin("ts", wallTime)));
        for (Measurement measurement : meter.measure()) {
            double value = measurement.getValue();
            if (!Double.isFinite(value)) {
                continue;
            }
            ops.add(Operation.put(new Bin(measurement.getStatistic().getTagValueRepresentation(), value)));
        }
        addTagOps(meter.getId(), ops);

        return Optional.of(new BatchWrite(
                config.batchWritePolicy(),
                getKey(name, wallTime),
                ops.toArray(new Operation[0])
        ));
    }

    Key getKey(String meterName, long timestamp) {
        return new Key(config.namespace(), config.setName(), meterName + "_" + timestamp);
    }

    void addTagOps(Meter.Id id, final List<Operation> ops) {
        getConventionTags(id).stream().map(t ->
                Operation.put(new Bin("__" + t.getKey(), t.getValue()))).forEach(ops::add);
    }

    @Override
    @Nonnull
    protected TimeUnit getBaseTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }
}
