package org.embedded.dfs.key.value.storage.memstore.api;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.WritableComparator;
import org.embedded.client.api.Storage;
import org.embedded.dfs.key.value.storage.memstore.api.impl.SkipListBasedMemstore;
import org.embedded.dfs.key.value.storage.wal.api.WAL;
import org.embedded.key.value.api.Key;
import org.embedded.key.value.api.KeyValue;
import org.embedded.operation.api.Operation;

import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Created by aakash on 9/8/17.
 */
public interface Memstore extends AutoCloseable {
    void init(Storage storage, WAL wal, long upperSizeLimit);

    WAL getWal();

    long size();

    long count();

    boolean isClosed();

    Storage getStorage();

    boolean put(KeyValue keyValue, Operation operation);

    Entry<SkipListBasedMemstore.ValueWithTimestamp> get(Key key);

    Stream<Entry<KeyValue>> stream();

    boolean shouldNotifyLevelZeroCompactor();


    class Entry<T> {
        private final T entity;
        private final Operation operation;


        public Entry(T entity, Operation operation) {
            Preconditions.checkNotNull(entity);
            Preconditions.checkNotNull(operation);
            this.entity = entity;
            this.operation = operation;
        }

        public T getEntity() {
            return this.entity;
        }

        public Operation getOperation() {
            return operation;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Entry entry = (Entry) o;

            if (entity != null ? !entity.equals(entry.entity) : entry.entity != null) return false;
            return operation == entry.operation;
        }

        @Override
        public int hashCode() {
            int result = entity != null ? entity.hashCode() : 0;
            result = 31 * result + (operation != null ? operation.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "entity=" + entity +
                    ", operation=" + operation +
                    '}';
        }
    }

    class KeyComparator<T extends Key> implements Comparator<T> {

        @Override
        public int compare(T o1, T o2) {
            byte[] b1 = o1.getValue();
            byte[] b2 = o2.getValue();
            return WritableComparator.compareBytes(b1, 0, b1.length, b2, 0, b2.length);
        }
    }
}
