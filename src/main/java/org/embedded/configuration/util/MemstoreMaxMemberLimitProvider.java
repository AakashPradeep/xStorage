package org.embedded.configuration.util;

import org.apache.hadoop.conf.Configuration;

import java.util.function.Function;

/**
 * Created by aakash on 9/10/17.
 */
public class MemstoreMaxMemberLimitProvider implements Function<Configuration, Integer> {
    @Override
    public Integer apply(Configuration configuration) {
        return configuration.getInt("memstore.lru.cache.max.member.limit.per.storage",
                10);
    }
}
