package org.embedded.key.value.api;

/**
 * Created by aakash on 9/7/17.
 */
public class Key extends BytesValue {

    public static Key from(byte[] bytes) {
        return new Key(bytes);
    }

    public Key(byte[] bytes) {
        super(bytes, false);
    }
}
