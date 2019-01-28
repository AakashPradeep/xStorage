package org.embedded.dfs.key.value.storage.memstore.api.impl;

import org.embedded.client.api.Storage;
import org.embedded.dfs.key.value.storage.memstore.api.Memstore;
import org.embedded.dfs.key.value.storage.wal.api.WAL;
import org.embedded.key.value.api.Key;
import org.embedded.key.value.api.KeyValue;
import org.embedded.key.value.api.Value;
import org.embedded.operation.api.Operation;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Created by aakash on 9/8/17.
 */
public class SkipListBasedMemstore implements Memstore {
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicLong count = new AtomicLong(0);
    private final AtomicLong size = new AtomicLong(0);
    private final ConcurrentSkipListMap<Key, Entry<ValueWithTimestamp>> skipListMap = new
            ConcurrentSkipListMap<>(new Memstore.KeyComparator<>());
    private Storage storage;
    private WAL wal;
    private long upperSizeLimit = Integer.MAX_VALUE;

    @Override
    public void init(Storage storage, WAL wal, long upperSizeLimit) {
        this.storage = storage;
        this.wal = wal;
        this.upperSizeLimit = upperSizeLimit;
    }

    @Override
    public WAL getWal() {
        return this.wal;
    }

    @Override
    public long size() {
        return size.get();
    }

    @Override
    public long count() {
        return this.count.get();
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    public void setWal(WAL wal) {
        this.wal = wal;
    }

    @Override
    public Storage getStorage() {
        return this.storage;
    }

    @Override
    public boolean put(KeyValue keyValue, Operation operation) {
        if (!isClosed()) {
            skipListMap.put(keyValue.key(), new Entry<>(ValueWithTimestamp.from(keyValue), operation));
            size.addAndGet(keyValue.size());
            count.incrementAndGet();
            return true;
        }
        return false;
    }

    @Override
    public Entry<ValueWithTimestamp> get(Key key) {
        return this.skipListMap.get(key);
    }

    @Override
    public Stream<Entry<KeyValue>> stream() {
        return this.skipListMap.entrySet().stream().map(e ->
        {
            ValueWithTimestamp valueWithTimestamp = e.getValue().getEntity();
            return new Entry<>(new KeyValue(e.getKey(), valueWithTimestamp.getValue(), valueWithTimestamp.getTimestamp()),
                    e.getValue().getOperation());
        });
    }

    @Override
    public boolean shouldNotifyLevelZeroCompactor() {
        return size.get() > 0 && size.get() >= upperSizeLimit;
    }

    @Override
    public void close() throws Exception {
        this.isClosed.compareAndSet(false, true);
        getWal().close();
    }

    /**
     * Defining a new bean to store value and timestamp to save the space as key will be stored as key of
     * the map so avoiding to have it in value of Map.
     */
    public static class ValueWithTimestamp {
        private final Value value;
        private final long timestamp;

        public ValueWithTimestamp(Value value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }

        public static ValueWithTimestamp from(KeyValue keyValue) {
            return new ValueWithTimestamp(keyValue.value(), keyValue.timestamp());
        }

        public Value getValue() {
            return value;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
}
