package org.embedded.key.value.api;

import com.google.common.base.Preconditions;

/**
 * Created by aakash on 9/8/17.
 */
public class KeyValue {
    private final Key key;
    private final Value value;
    private final long timestamp;
    private final long size;

    public KeyValue(Key key, Value value, long timestamp) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        Preconditions.checkArgument(timestamp > 0);

        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.size = this.key.size() + this.value.size();
    }

    public Key key() {
        return key;
    }

    public Value value() {
        return value;
    }

    public long size() {
        return size;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyValue keyValue = (KeyValue) o;

        return key != null ? key.equals(keyValue.key) : keyValue.key == null;
    }

    @Override
    public int hashCode() {
        return key != null ? key.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "KeyValue{" +
                "key=" + key +
                ", value=" + value +
                ", timestamp=" + timestamp +
                ", size=" + size +
                '}';
    }
}
