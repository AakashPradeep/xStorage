package org.embedded.operation.api;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Created by aakash on 9/9/17.
 */
public class OperationProvider {
    static Map<Integer, Operation> ordinalToOperationMap = Maps.newHashMap();

    static {
        for (Operation operation : Operation.values()) {
            ordinalToOperationMap.put(operation.ordinal(), operation);
        }
    }

    public static Operation fromOrdinal(int ordinal) {
        return ordinalToOperationMap.get(ordinal);
    }
}
