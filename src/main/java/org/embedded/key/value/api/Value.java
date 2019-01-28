package org.embedded.key.value.api;

/**
 * Created by aakash on 9/7/17.
 */
public class Value extends BytesValue {

    public static Value ofNullable() {
        return new Value(null);
    }

    public static Value from(byte[] bytes) {
        return new Value(bytes);
    }

    public Value(byte[] bytes) {
        super(bytes, true);
    }
}
