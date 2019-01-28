package org.embedded.configuration.util;

import org.apache.hadoop.conf.Configuration;

import java.util.function.Function;

/**
 * Created by aakash on 9/10/17.
 */
public class MemstoreMaxSizeLimitProvider implements Function<Configuration, Integer> {
    @Override
    public Integer apply(Configuration configuration) {
        return configuration.getInt("memstore.lru.max.size.limit.in.bytes.per.storage",
                8 * 1024 * 1024 /*8MB*/);
    }
}
