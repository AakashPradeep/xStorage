package org.embedded.dfs.key.value.storage.wal.api;

import com.google.common.base.Preconditions;
import org.embedded.client.api.Storage;
import org.embedded.key.value.api.KeyValue;
import org.embedded.operation.api.Operation;

import java.io.IOException;

/**
 * Created by aakash on 9/7/17.
 */
public interface WAL extends AutoCloseable {
    void init(Storage storage, String rootPath, WALFilePathProvider filePathProvider, long lastSequenceId);

    Storage getStorage();

    String getRootPath();

    String walFilePath();

    long getCurrentSequenceId();

    boolean append(KeyValue keyValue, Operation operation) throws IOException;


    interface Writer<T extends Entry> extends AutoCloseable {
        boolean write(T entry) throws IOException;
    }

    interface Reader<T extends Entry> extends Iterable<T>, AutoCloseable {
        String getFilePath();
    }

    class Entry {
        private final long sequenceId;
        private final KeyValue keyValue;
        private final Operation operation;


        public Entry(long sequenceId, KeyValue keyValue, Operation operation) {
            Preconditions.checkNotNull(keyValue);
            Preconditions.checkNotNull(operation);
            Preconditions.checkArgument(sequenceId > 0, "sequence Id must be a positive number");
            this.sequenceId = sequenceId;
            this.keyValue = keyValue;
            this.operation = operation;
        }

        public long getSequenceId() {
            return sequenceId;
        }

        public KeyValue getKeyValue() {
            return keyValue;
        }

        public Operation getOperation() {
            return operation;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Entry entry = (Entry) o;

            if (sequenceId != entry.sequenceId) return false;
            if (keyValue != null ? !keyValue.equals(entry.keyValue) : entry.keyValue != null) return false;
            return operation == entry.operation;
        }

        @Override
        public int hashCode() {
            int result = (int) (sequenceId ^ (sequenceId >>> 32));
            result = 31 * result + (keyValue != null ? keyValue.hashCode() : 0);
            result = 31 * result + (operation != null ? operation.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "sequenceId=" + sequenceId +
                    ", keyValue=" + keyValue +
                    ", operation=" + operation +
                    '}';
        }
    }
}