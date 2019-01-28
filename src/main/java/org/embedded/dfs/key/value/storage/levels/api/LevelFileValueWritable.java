package org.embedded.dfs.key.value.storage.levels.api;

import org.apache.hadoop.io.Writable;
import org.embedded.key.value.api.KeyValue;
import org.embedded.key.value.api.Value;
import org.embedded.operation.api.Operation;
import org.embedded.operation.api.OperationProvider;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by aakash on 9/9/17.
 */
public class LevelFileValueWritable implements Writable {
    private Wrapper wrapper = null;

    public LevelFileValueWritable(KeyValue keyValue, Operation operation) {
        this.wrapper = new Wrapper(keyValue.value(), keyValue.timestamp(), operation);
    }

    public LevelFileValueWritable() {
    }

    @Override
    public void write(DataOutput out) throws IOException {

        byte[] value = wrapper.getValue().getValue();
        out.writeInt(value.length);
        out.write(value);
        out.writeLong(wrapper.getTimestamp());
        out.writeByte(wrapper.getOperation().ordinal());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int valueLength = in.readInt();
        byte[] value = new byte[valueLength];
        in.readFully(value);
        long timestamp = in.readLong();
        byte operationOrdinal = in.readByte();
        Operation operation = OperationProvider.fromOrdinal(operationOrdinal);
        wrapper = new Wrapper(new Value(value), timestamp, operation);
    }

    public Value getValue() {
        return this.wrapper != null ? this.wrapper.getValue() : Value.ofNullable();
    }

    public long getTimestamp() {
        return this.wrapper != null ? this.wrapper.getTimestamp() : -1;
    }

    public Operation getOperation() {
        return this.wrapper != null ? this.wrapper.getOperation() : null;
    }

    private static class Wrapper {
        private final Value value;
        private final long timestamp;
        private final Operation operation;


        private Wrapper(Value value, long timestamp, Operation operation) {
            this.value = value;
            this.timestamp = timestamp;
            this.operation = operation;
        }

        public Value getValue() {
            return value;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public Operation getOperation() {
            return operation;
        }
    }
}
