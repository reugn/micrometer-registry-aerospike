package io.github.reugn.micrometer.aerospike.client;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.listener.*;
import com.aerospike.client.policy.*;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.QueryListener;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.Calendar;
import java.util.List;
import java.util.Objects;

public class AerospikeClientMicrometer implements IAerospikeClient {

    private final IAerospikeClient delegate;
    private final MeterRegistry registry;
    private final MicrometerPolicy micrometerPolicy;

    public AerospikeClientMicrometer(IAerospikeClient delegate, MeterRegistry registry) {
        this(delegate, registry, MicrometerPolicy.DEFAULT);
    }

    public AerospikeClientMicrometer(IAerospikeClient delegate, MeterRegistry registry, MicrometerPolicy micrometerPolicy) {
        this.delegate = Objects.requireNonNull(delegate, "delegate is null");
        this.registry = Objects.requireNonNull(registry, "registry is null");
        this.micrometerPolicy = Objects.requireNonNull(micrometerPolicy, "micrometerPolicy is null");
    }

    private void countMethodCalls(String methodName) {
        if (micrometerPolicy.isMeterMethodCalls()) {
            Counter.builder("client." + methodName + ".count").register(registry).increment();
        }
    }

    private void countErrors() {
        if (micrometerPolicy.isMeterErrors()) {
            Counter.builder("client.error.count").register(registry).increment();
        }
    }

    private Timer timer(String methodName) {
        return Timer.builder("client." + methodName + ".time").register(registry);
    }

    @Override
    public Policy getReadPolicyDefault() {
        return delegate.getReadPolicyDefault();
    }

    @Override
    public WritePolicy getWritePolicyDefault() {
        return delegate.getWritePolicyDefault();
    }

    @Override
    public ScanPolicy getScanPolicyDefault() {
        return delegate.getScanPolicyDefault();
    }

    @Override
    public QueryPolicy getQueryPolicyDefault() {
        return delegate.getQueryPolicyDefault();
    }

    @Override
    public BatchPolicy getBatchPolicyDefault() {
        return delegate.getBatchPolicyDefault();
    }

    @Override
    public BatchPolicy getBatchParentPolicyWriteDefault() {
        return delegate.getBatchParentPolicyWriteDefault();
    }

    @Override
    public BatchWritePolicy getBatchWritePolicyDefault() {
        return delegate.getBatchWritePolicyDefault();
    }

    @Override
    public BatchDeletePolicy getBatchDeletePolicyDefault() {
        return delegate.getBatchDeletePolicyDefault();
    }

    @Override
    public BatchUDFPolicy getBatchUDFPolicyDefault() {
        return delegate.getBatchUDFPolicyDefault();
    }

    @Override
    public InfoPolicy getInfoPolicyDefault() {
        return delegate.getInfoPolicyDefault();
    }

    @Override
    public void close() {
        countMethodCalls("close");
        delegate.close();
    }

    @Override
    public boolean isConnected() {
        countMethodCalls("isConnected");
        return delegate.isConnected();
    }

    @Override
    public Node[] getNodes() {
        countMethodCalls("getNodes");
        return delegate.getNodes();
    }

    @Override
    public List<String> getNodeNames() {
        countMethodCalls("getNodeNames");
        return delegate.getNodeNames();
    }

    @Override
    public Node getNode(String nodeName) throws AerospikeException.InvalidNode {
        countMethodCalls("getNode");
        try {
            return delegate.getNode(nodeName);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public ClusterStats getClusterStats() {
        return delegate.getClusterStats();
    }

    @Override
    public Cluster getCluster() {
        return delegate.getCluster();
    }

    @Override
    public void put(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        countMethodCalls("put");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                timer("put").record(() -> delegate.put(policy, key, bins));
                return;
            }
            delegate.put(policy, key, bins);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void put(EventLoop eventLoop, WriteListener listener, WritePolicy policy,
                    Key key, Bin... bins) throws AerospikeException {
        countMethodCalls("put");
        try {
            delegate.put(eventLoop, listener, policy, key, bins);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void append(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        countMethodCalls("append");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                timer("append").record(() -> delegate.append(policy, key, bins));
                return;
            }
            delegate.append(policy, key, bins);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void append(EventLoop eventLoop, WriteListener listener, WritePolicy policy,
                       Key key, Bin... bins) throws AerospikeException {
        countMethodCalls("append");
        try {
            delegate.append(eventLoop, listener, policy, key, bins);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void prepend(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        countMethodCalls("prepend");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                timer("prepend").record(() -> delegate.prepend(policy, key, bins));
                return;
            }
            delegate.prepend(policy, key, bins);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void prepend(EventLoop eventLoop, WriteListener listener, WritePolicy policy,
                        Key key, Bin... bins) throws AerospikeException {
        countMethodCalls("prepend");
        try {
            delegate.prepend(eventLoop, listener, policy, key, bins);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void add(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        countMethodCalls("add");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                timer("add").record(() -> delegate.add(policy, key, bins));
                return;
            }
            delegate.add(policy, key, bins);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void add(EventLoop eventLoop, WriteListener listener, WritePolicy policy,
                    Key key, Bin... bins) throws AerospikeException {
        countMethodCalls("add");
        try {
            delegate.add(eventLoop, listener, policy, key, bins);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public boolean delete(WritePolicy policy, Key key) throws AerospikeException {
        countMethodCalls("delete");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("delete").record(
                        () -> delegate.delete(policy, key)
                );
            }
            return delegate.delete(policy, key);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void delete(EventLoop eventLoop, DeleteListener listener, WritePolicy policy,
                       Key key) throws AerospikeException {
        countMethodCalls("delete");
        try {
            delegate.delete(eventLoop, listener, policy, key);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public BatchResults delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy,
                               Key[] keys) throws AerospikeException {
        countMethodCalls("delete");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("delete").record(
                        () -> delegate.delete(batchPolicy, deletePolicy, keys)
                );
            }
            return delegate.delete(batchPolicy, deletePolicy, keys);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void delete(EventLoop eventLoop, BatchRecordArrayListener listener, BatchPolicy batchPolicy,
                       BatchDeletePolicy deletePolicy, Key[] keys) throws AerospikeException {
        countMethodCalls("delete");
        try {
            delegate.delete(eventLoop, listener, batchPolicy, deletePolicy, keys);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void delete(EventLoop eventLoop, BatchRecordSequenceListener listener, BatchPolicy batchPolicy,
                       BatchDeletePolicy deletePolicy, Key[] keys) throws AerospikeException {
        countMethodCalls("delete");
        try {
            delegate.delete(eventLoop, listener, batchPolicy, deletePolicy, keys);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void truncate(InfoPolicy policy, String ns, String set, Calendar beforeLastUpdate) throws AerospikeException {
        countMethodCalls("truncate");
        try {
            delegate.truncate(policy, ns, set, beforeLastUpdate);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void touch(WritePolicy policy, Key key) throws AerospikeException {
        countMethodCalls("touch");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                timer("touch").record(() -> delegate.touch(policy, key));
                return;
            }
            delegate.touch(policy, key);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void touch(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key) throws AerospikeException {
        countMethodCalls("touch");
        try {
            delegate.touch(eventLoop, listener, policy, key);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public boolean exists(Policy policy, Key key) throws AerospikeException {
        countMethodCalls("exists");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("exists").record(
                        () -> delegate.exists(policy, key)
                );
            }
            return delegate.exists(policy, key);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsListener listener, Policy policy, Key key) throws AerospikeException {
        countMethodCalls("exists");
        try {
            delegate.exists(eventLoop, listener, policy, key);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public boolean[] exists(BatchPolicy policy, Key[] keys) throws AerospikeException {
        countMethodCalls("exists");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("exists").record(
                        () -> delegate.exists(policy, keys)
                );
            }
            return delegate.exists(policy, keys);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsArrayListener listener, BatchPolicy policy,
                       Key[] keys) throws AerospikeException {
        countMethodCalls("exists");
        try {
            delegate.exists(eventLoop, listener, policy, keys);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsSequenceListener listener, BatchPolicy policy,
                       Key[] keys) throws AerospikeException {
        countMethodCalls("exists");
        try {
            delegate.exists(eventLoop, listener, policy, keys);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public Record get(Policy policy, Key key) throws AerospikeException {
        countMethodCalls("get");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("get").record(
                        () -> delegate.get(policy, key)
                );
            }
            return delegate.get(policy, key);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) throws AerospikeException {
        countMethodCalls("get");
        try {
            delegate.get(eventLoop, listener, policy, key);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public Record get(Policy policy, Key key, String... binNames) throws AerospikeException {
        countMethodCalls("get");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("get").record(
                        () -> delegate.get(policy, key, binNames)
                );
            }
            return delegate.get(policy, key, binNames);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void get(EventLoop eventLoop, RecordListener listener, Policy policy,
                    Key key, String... binNames) throws AerospikeException {
        countMethodCalls("get");
        try {
            delegate.get(eventLoop, listener, policy, key, binNames);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public Record getHeader(Policy policy, Key key) throws AerospikeException {
        countMethodCalls("getHeader");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("getHeader").record(
                        () -> delegate.getHeader(policy, key)
                );
            }
            return delegate.getHeader(policy, key);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordListener listener, Policy policy,
                          Key key) throws AerospikeException {
        countMethodCalls("getHeader");
        try {
            delegate.getHeader(eventLoop, listener, policy, key);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public boolean get(BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
        countMethodCalls("get");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("get").record(
                        () -> delegate.get(policy, records)
                );
            }
            return delegate.get(policy, records);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void get(EventLoop eventLoop, BatchListListener listener, BatchPolicy policy,
                    List<BatchRead> records) throws AerospikeException {
        countMethodCalls("get");
        try {
            delegate.get(eventLoop, listener, policy, records);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void get(EventLoop eventLoop, BatchSequenceListener listener, BatchPolicy policy,
                    List<BatchRead> records) throws AerospikeException {
        countMethodCalls("get");
        try {
            delegate.get(eventLoop, listener, policy, records);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public Record[] get(BatchPolicy policy, Key[] keys) throws AerospikeException {
        countMethodCalls("get");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("get").record(
                        () -> delegate.get(policy, keys)
                );
            }
            return delegate.get(policy, keys);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy,
                    Key[] keys) throws AerospikeException {
        countMethodCalls("get");
        try {
            delegate.get(eventLoop, listener, policy, keys);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy,
                    Key[] keys) throws AerospikeException {
        countMethodCalls("get");
        try {
            delegate.get(eventLoop, listener, policy, keys);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public Record[] get(BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException {
        countMethodCalls("get");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("get").record(
                        () -> delegate.get(policy, keys, binNames)
                );
            }
            return delegate.get(policy, keys, binNames);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys,
                    String... binNames) throws AerospikeException {
        countMethodCalls("get");
        try {
            delegate.get(eventLoop, listener, policy, keys, binNames);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys,
                    String... binNames) throws AerospikeException {
        countMethodCalls("get");
        try {
            delegate.get(eventLoop, listener, policy, keys, binNames);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public Record[] get(BatchPolicy policy, Key[] keys, Operation... ops) throws AerospikeException {
        countMethodCalls("get");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("get").record(
                        () -> delegate.get(policy, keys, ops)
                );
            }
            return delegate.get(policy, keys, ops);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys,
                    Operation... ops) throws AerospikeException {
        countMethodCalls("get");
        try {
            delegate.get(eventLoop, listener, policy, keys, ops);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys,
                    Operation... ops) throws AerospikeException {
        countMethodCalls("get");
        try {
            delegate.get(eventLoop, listener, policy, keys, ops);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public Record[] getHeader(BatchPolicy policy, Key[] keys) throws AerospikeException {
        countMethodCalls("getHeader");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("getHeader").record(
                        () -> delegate.getHeader(policy, keys)
                );
            }
            return delegate.getHeader(policy, keys);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy,
                          Key[] keys) throws AerospikeException {
        countMethodCalls("getHeader");
        try {
            delegate.get(eventLoop, listener, policy, keys);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy,
                          Key[] keys) throws AerospikeException {
        countMethodCalls("getHeader");
        try {
            delegate.get(eventLoop, listener, policy, keys);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public Record operate(WritePolicy policy, Key key, Operation... operations) throws AerospikeException {
        countMethodCalls("operate");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("operate").record(
                        () -> delegate.operate(policy, key, operations)
                );
            }
            return delegate.operate(policy, key, operations);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void operate(EventLoop eventLoop, RecordListener listener, WritePolicy policy, Key key,
                        Operation... operations) throws AerospikeException {
        countMethodCalls("operate");
        try {
            delegate.operate(eventLoop, listener, policy, key, operations);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public boolean operate(BatchPolicy policy, List<BatchRecord> records) throws AerospikeException {
        countMethodCalls("operate");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("operate").record(
                        () -> delegate.operate(policy, records)
                );
            }
            return delegate.operate(policy, records);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void operate(EventLoop eventLoop, BatchOperateListListener listener, BatchPolicy policy,
                        List<BatchRecord> records) throws AerospikeException {
        countMethodCalls("operate");
        try {
            delegate.operate(eventLoop, listener, policy, records);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void operate(EventLoop eventLoop, BatchRecordSequenceListener listener, BatchPolicy policy,
                        List<BatchRecord> records) throws AerospikeException {
        countMethodCalls("operate");
        try {
            delegate.operate(eventLoop, listener, policy, records);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public BatchResults operate(BatchPolicy batchPolicy, BatchWritePolicy writePolicy, Key[] keys,
                                Operation... ops) throws AerospikeException {
        countMethodCalls("operate");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("operate").record(
                        () -> delegate.operate(batchPolicy, writePolicy, keys, ops)
                );
            }
            return delegate.operate(batchPolicy, writePolicy, keys, ops);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void operate(EventLoop eventLoop, BatchRecordArrayListener listener, BatchPolicy batchPolicy,
                        BatchWritePolicy writePolicy, Key[] keys, Operation... ops) throws AerospikeException {
        countMethodCalls("operate");
        try {
            delegate.operate(eventLoop, listener, batchPolicy, writePolicy, keys, ops);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void operate(EventLoop eventLoop, BatchRecordSequenceListener listener, BatchPolicy batchPolicy,
                        BatchWritePolicy writePolicy, Key[] keys, Operation... ops) throws AerospikeException {
        countMethodCalls("operate");
        try {
            delegate.operate(eventLoop, listener, batchPolicy, writePolicy, keys, ops);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void scanAll(ScanPolicy policy, String namespace, String setName, ScanCallback callback,
                        String... binNames) throws AerospikeException {
        countMethodCalls("scanAll");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                timer("scanAll").record(
                        () -> delegate.scanAll(policy, namespace, setName, callback, binNames)
                );
                return;
            }
            delegate.scanAll(policy, namespace, setName, callback, binNames);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void scanAll(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy, String namespace,
                        String setName, String... binNames) throws AerospikeException {
        countMethodCalls("scanAll");
        try {
            delegate.scanAll(eventLoop, listener, policy, namespace, setName, binNames);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void scanNode(ScanPolicy policy, String nodeName, String namespace, String setName, ScanCallback callback,
                         String... binNames) throws AerospikeException {
        countMethodCalls("scanNode");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                timer("scanNode").record(
                        () -> delegate.scanNode(policy, nodeName, namespace, setName, callback, binNames)
                );
                return;
            }
            delegate.scanNode(policy, nodeName, namespace, setName, callback, binNames);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback,
                         String... binNames) throws AerospikeException {
        countMethodCalls("scanNode");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                timer("scanNode").record(
                        () -> delegate.scanNode(policy, node, namespace, setName, callback, binNames)
                );
                return;
            }
            delegate.scanNode(policy, node, namespace, setName, callback, binNames);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void scanPartitions(ScanPolicy policy, PartitionFilter partitionFilter, String namespace, String setName,
                               ScanCallback callback, String... binNames) throws AerospikeException {
        countMethodCalls("scanPartitions");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                timer("scanPartitions").record(
                        () -> delegate.scanPartitions(policy, partitionFilter, namespace, setName, callback, binNames)
                );
                return;
            }
            delegate.scanPartitions(policy, partitionFilter, namespace, setName, callback, binNames);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void scanPartitions(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy,
                               PartitionFilter partitionFilter, String namespace, String setName,
                               String... binNames) throws AerospikeException {
        countMethodCalls("scanPartitions");
        try {
            delegate.scanPartitions(eventLoop, listener, policy, partitionFilter, namespace, setName, binNames);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public RegisterTask register(Policy policy, String clientPath, String serverPath,
                                 Language language) throws AerospikeException {
        countMethodCalls("register");
        try {
            return delegate.register(policy, clientPath, serverPath, language);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public RegisterTask register(Policy policy, ClassLoader resourceLoader, String resourcePath,
                                 String serverPath, Language language) throws AerospikeException {
        countMethodCalls("register");
        try {
            return delegate.register(policy, resourceLoader, resourcePath, serverPath, language);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public RegisterTask registerUdfString(Policy policy, String code, String serverPath,
                                          Language language) throws AerospikeException {
        countMethodCalls("registerUdfString");
        try {
            return delegate.registerUdfString(policy, code, serverPath, language);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void removeUdf(InfoPolicy policy, String serverPath) throws AerospikeException {
        countMethodCalls("removeUdf");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                timer("removeUdf").record(
                        () -> delegate.removeUdf(policy, serverPath)
                );
                return;
            }
            delegate.removeUdf(policy, serverPath);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public Object execute(WritePolicy policy, Key key, String packageName, String functionName,
                          Value... args) throws AerospikeException {
        countMethodCalls("execute");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("execute").record(
                        () -> delegate.execute(policy, key, packageName, functionName, args)
                );
            }
            return delegate.execute(policy, key, packageName, functionName, args);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void execute(EventLoop eventLoop, ExecuteListener listener, WritePolicy policy, Key key,
                        String packageName, String functionName, Value... functionArgs) throws AerospikeException {
        countMethodCalls("execute");
        try {
            delegate.execute(eventLoop, listener, policy, key, packageName, functionName, functionArgs);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public BatchResults execute(BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, Key[] keys,
                                String packageName, String functionName, Value... functionArgs) throws AerospikeException {
        countMethodCalls("execute");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("execute").record(
                        () -> delegate.execute(batchPolicy, udfPolicy, keys, packageName, functionName, functionArgs)
                );
            }
            return delegate.execute(batchPolicy, udfPolicy, keys, packageName, functionName, functionArgs);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void execute(EventLoop eventLoop, BatchRecordArrayListener listener, BatchPolicy batchPolicy,
                        BatchUDFPolicy udfPolicy, Key[] keys, String packageName, String functionName,
                        Value... functionArgs) throws AerospikeException {
        countMethodCalls("execute");
        try {
            delegate.execute(eventLoop, listener, batchPolicy, udfPolicy, keys, packageName, functionName, functionArgs);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void execute(EventLoop eventLoop, BatchRecordSequenceListener listener, BatchPolicy batchPolicy,
                        BatchUDFPolicy udfPolicy, Key[] keys, String packageName, String functionName,
                        Value... functionArgs) throws AerospikeException {
        countMethodCalls("execute");
        try {
            delegate.execute(eventLoop, listener, batchPolicy, udfPolicy, keys, packageName, functionName, functionArgs);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public ExecuteTask execute(WritePolicy policy, Statement statement, String packageName, String functionName,
                               Value... functionArgs) throws AerospikeException {
        countMethodCalls("execute");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("execute").record(
                        () -> delegate.execute(policy, statement, packageName, functionName, functionArgs)
                );
            }
            return delegate.execute(policy, statement, packageName, functionName, functionArgs);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public ExecuteTask execute(WritePolicy policy, Statement statement, Operation... operations) throws AerospikeException {
        countMethodCalls("execute");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("execute").record(
                        () -> delegate.execute(policy, statement, operations)
                );
            }
            return delegate.execute(policy, statement, operations);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public RecordSet query(QueryPolicy policy, Statement statement) throws AerospikeException {
        countMethodCalls("query");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("query").record(
                        () -> delegate.query(policy, statement)
                );
            }
            return delegate.query(policy, statement);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void query(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy,
                      Statement statement) throws AerospikeException {
        countMethodCalls("query");
        try {
            delegate.query(eventLoop, listener, policy, statement);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void query(QueryPolicy policy, Statement statement, QueryListener listener) throws AerospikeException {
        countMethodCalls("query");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                timer("query").record(
                        () -> delegate.query(policy, statement, listener)
                );
                return;
            }
            delegate.query(policy, statement, listener);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void query(QueryPolicy policy, Statement statement, PartitionFilter partitionFilter,
                      QueryListener listener) throws AerospikeException {
        countMethodCalls("query");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                timer("query").record(
                        () -> delegate.query(policy, statement, partitionFilter, listener)
                );
                return;
            }
            delegate.query(policy, statement, partitionFilter, listener);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public RecordSet queryNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
        countMethodCalls("queryNode");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("queryNode").record(
                        () -> delegate.queryNode(policy, statement, node)
                );
            }
            return delegate.queryNode(policy, statement, node);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public RecordSet queryPartitions(QueryPolicy policy, Statement statement,
                                     PartitionFilter partitionFilter) throws AerospikeException {
        countMethodCalls("queryPartitions");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("queryPartitions").record(
                        () -> delegate.queryPartitions(policy, statement, partitionFilter)
                );
            }
            return delegate.queryPartitions(policy, statement, partitionFilter);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void queryPartitions(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy,
                                Statement statement, PartitionFilter partitionFilter) throws AerospikeException {
        countMethodCalls("queryPartitions");
        try {
            delegate.queryPartitions(eventLoop, listener, policy, statement, partitionFilter);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public ResultSet queryAggregate(QueryPolicy policy, Statement statement, String packageName,
                                    String functionName, Value... functionArgs) throws AerospikeException {
        countMethodCalls("queryAggregate");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("queryAggregate").record(
                        () -> delegate.queryAggregate(policy, statement, packageName, functionName, functionArgs)
                );
            }
            return delegate.queryAggregate(policy, statement, packageName, functionName, functionArgs);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public ResultSet queryAggregate(QueryPolicy policy, Statement statement) throws AerospikeException {
        countMethodCalls("queryAggregate");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("queryAggregate").record(
                        () -> delegate.queryAggregate(policy, statement)
                );
            }
            return delegate.queryAggregate(policy, statement);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public ResultSet queryAggregateNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
        countMethodCalls("queryAggregateNode");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("queryAggregateNode").record(
                        () -> delegate.queryAggregateNode(policy, statement, node)
                );
            }
            return delegate.queryAggregateNode(policy, statement, node);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName,
                                 IndexType indexType) throws AerospikeException {
        countMethodCalls("createIndex");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("createIndex").record(
                        () -> delegate.createIndex(policy, namespace, setName, indexName, binName, indexType)
                );
            }
            return delegate.createIndex(policy, namespace, setName, indexName, binName, indexType);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName,
                                 IndexType indexType, IndexCollectionType indexCollectionType) throws AerospikeException {
        countMethodCalls("createIndex");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("createIndex").record(
                        () -> delegate.createIndex(policy, namespace, setName, indexName, binName, indexType, indexCollectionType)
                );
            }
            return delegate.createIndex(policy, namespace, setName, indexName, binName, indexType, indexCollectionType);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void createIndex(EventLoop eventLoop, IndexListener listener, Policy policy, String namespace,
                            String setName, String indexName, String binName, IndexType indexType,
                            IndexCollectionType indexCollectionType) throws AerospikeException {
        countMethodCalls("createIndex");
        try {
            delegate.createIndex(eventLoop, listener, policy, namespace, setName, indexName, binName, indexType, indexCollectionType);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public IndexTask dropIndex(Policy policy, String namespace, String setName,
                               String indexName) throws AerospikeException {
        countMethodCalls("dropIndex");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                return timer("dropIndex").record(
                        () -> delegate.dropIndex(policy, namespace, setName, indexName)
                );
            }
            return delegate.dropIndex(policy, namespace, setName, indexName);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void dropIndex(EventLoop eventLoop, IndexListener listener, Policy policy, String namespace,
                          String setName, String indexName) throws AerospikeException {
        countMethodCalls("dropIndex");
        try {
            delegate.dropIndex(eventLoop, listener, policy, namespace, setName, indexName);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void info(EventLoop eventLoop, InfoListener listener, InfoPolicy policy, Node node,
                     String... commands) throws AerospikeException {
        countMethodCalls("info");
        try {
            delegate.info(eventLoop, listener, policy, node, commands);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void setXDRFilter(InfoPolicy policy, String datacenter, String namespace,
                             Expression filter) throws AerospikeException {
        countMethodCalls("setXDRFilter");
        try {
            if (micrometerPolicy.isMeterMethodTime()) {
                timer("setXDRFilter").record(
                        () -> delegate.setXDRFilter(policy, datacenter, namespace, filter)
                );
                return;
            }
            delegate.setXDRFilter(policy, datacenter, namespace, filter);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void createUser(AdminPolicy policy, String user, String password, List<String> roles) throws AerospikeException {
        countMethodCalls("createUser");
        try {
            delegate.createUser(policy, user, password, roles);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void dropUser(AdminPolicy policy, String user) throws AerospikeException {
        countMethodCalls("dropUser");
        try {
            delegate.dropUser(policy, user);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void changePassword(AdminPolicy policy, String user, String password) throws AerospikeException {
        countMethodCalls("changePassword");
        try {
            delegate.changePassword(policy, user, password);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void grantRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
        countMethodCalls("grantRoles");
        try {
            delegate.grantRoles(policy, user, roles);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void revokeRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
        countMethodCalls("revokeRoles");
        try {
            delegate.revokeRoles(policy, user, roles);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
        countMethodCalls("createRole");
        try {
            delegate.createRole(policy, roleName, privileges);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges,
                           List<String> whitelist) throws AerospikeException {
        countMethodCalls("createRole");
        try {
            delegate.createRole(policy, roleName, privileges, whitelist);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges, List<String> whitelist,
                           int readQuota, int writeQuota) throws AerospikeException {
        countMethodCalls("createRole");
        try {
            delegate.createRole(policy, roleName, privileges, whitelist, readQuota, writeQuota);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void dropRole(AdminPolicy policy, String roleName) throws AerospikeException {
        countMethodCalls("dropRole");
        try {
            delegate.dropRole(policy, roleName);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void grantPrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
        countMethodCalls("grantPrivileges");
        try {
            delegate.grantPrivileges(policy, roleName, privileges);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void revokePrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
        countMethodCalls("revokePrivileges");
        try {
            delegate.revokePrivileges(policy, roleName, privileges);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void setWhitelist(AdminPolicy policy, String roleName, List<String> whitelist) throws AerospikeException {
        countMethodCalls("setWhitelist");
        try {
            delegate.setWhitelist(policy, roleName, whitelist);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public void setQuotas(AdminPolicy policy, String roleName, int readQuota, int writeQuota) throws AerospikeException {
        countMethodCalls("setQuotas");
        try {
            delegate.setQuotas(policy, roleName, readQuota, writeQuota);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public User queryUser(AdminPolicy policy, String user) throws AerospikeException {
        countMethodCalls("queryUser");
        try {
            return delegate.queryUser(policy, user);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public List<User> queryUsers(AdminPolicy policy) throws AerospikeException {
        countMethodCalls("queryUsers");
        try {
            return delegate.queryUsers(policy);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public Role queryRole(AdminPolicy policy, String roleName) throws AerospikeException {
        countMethodCalls("queryRole");
        try {
            return delegate.queryRole(policy, roleName);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }

    @Override
    public List<Role> queryRoles(AdminPolicy policy) throws AerospikeException {
        countMethodCalls("queryRoles");
        try {
            return delegate.queryRoles(policy);
        } catch (Throwable t) {
            countErrors();
            throw t;
        }
    }
}
