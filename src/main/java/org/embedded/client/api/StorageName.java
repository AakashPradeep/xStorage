package org.embedded.client.api;

import org.embedded.key.value.api.NonEmptyStringValue;

/**
 * Created by aakash on 9/7/17.
 */
public class StorageName extends NonEmptyStringValue {

    public static StorageName from(String storage) {
        return new StorageName(storage);
    }

    public StorageName(String value) {
        super(value);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj != null && obj instanceof StorageName) {
            return super.equals(obj);
        }
        return false;
    }
}
