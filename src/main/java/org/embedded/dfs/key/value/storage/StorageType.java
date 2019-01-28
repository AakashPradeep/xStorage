package org.embedded.dfs.key.value.storage;

import org.embedded.key.value.api.NonEmptyStringValue;

/**
 * Created by aakash on 9/8/17.
 */
public class StorageType extends NonEmptyStringValue {
    public static final StorageType WAL = new StorageType("WAL");
    public StorageType(String value) {
        super(value);
    }
}
