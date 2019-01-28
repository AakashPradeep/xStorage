package org.embedded.key.value.api;

import com.google.common.base.Preconditions;

import java.util.Arrays;

/**
 * Created by aakash on 9/7/17.
 */
public class BytesValue {
    private final byte[] value;
    private final boolean isNullable;
    private final long size;

    public BytesValue(byte[] value, boolean isNullable) {
        this.value = value;
        this.isNullable = isNullable;
        if (!isNullable) {
            Preconditions.checkArgument(this.value != null && this.value.length != 0, "value cannot be null or empty");
        }
        this.size = this.value != null ? this.value.length : 0;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.value);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof BytesValue) {
            return Arrays.equals(this.value, ((BytesValue) obj).value);
        }
        return false;
    }

    public long size() {
        return this.size;
    }

    public byte[] getValue() {
        return this.value != null ? Arrays.copyOf(this.value, this.value.length) : null;
    }

    public boolean isNullable() {
        return isNullable;
    }
}
