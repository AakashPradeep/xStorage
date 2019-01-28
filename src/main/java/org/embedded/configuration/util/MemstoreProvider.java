package org.embedded.configuration.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.embedded.dfs.key.value.storage.memstore.api.Memstore;
import org.embedded.dfs.key.value.storage.memstore.api.impl.SkipListBasedMemstore;

import java.util.function.Function;

/**
 * Created by aakash on 9/10/17.
 */
public class MemstoreProvider implements Function<Configuration, Memstore> {
    @Override
    public Memstore apply(Configuration configuration) {
        Class<?> aClass = configuration.getClass("memstore.impl.class", SkipListBasedMemstore.class);
        Memstore memstore = (Memstore) ReflectionUtils.newInstance(aClass, configuration);
        return memstore;
    }
}