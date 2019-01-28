package org.embedded.key.value.api;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

/**
 * Created by aakash on 9/7/17.
 */
public class NonEmptyStringValue {
    private final String value;

    public NonEmptyStringValue(String value) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(value), "value cannot be null or empty string.");
        this.value = value;
    }

    @Override
    public String toString() {
        return this.value;
    }

    @Override
    public int hashCode() {
        return this.value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return this.value.equals(obj);
    }
}
