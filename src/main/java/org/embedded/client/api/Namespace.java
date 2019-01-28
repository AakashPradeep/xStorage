package org.embedded.client.api;

import org.embedded.key.value.api.NonEmptyStringValue;

/**
 * Created by aakash on 9/7/17.
 */
public class Namespace extends NonEmptyStringValue {

    public static Namespace from(String namespace) {
        return new Namespace(namespace);
    }

    public Namespace(String value) {
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
        if (obj != null && obj instanceof Namespace) {
            return super.equals(obj);
        }
        return false;
    }
}
