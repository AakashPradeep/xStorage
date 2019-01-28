package org.embedded.configuration.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.embedded.client.api.Storage;
import org.embedded.dfs.key.value.storage.wal.api.WAL;
import org.embedded.dfs.key.value.storage.wal.api.WALFilePathProvider;
import org.embedded.dfs.key.value.storage.wal.api.avro.impl.AvroBasedWAL;

import java.util.function.BiFunction;

/**
 * Created by aakash on 9/10/17.
 */

public class WALProvider implements BiFunction<Configuration, Long, WAL> {

    private final Storage storage;

    public WALProvider(Storage storage) {
        this.storage = storage;
    }

    @Override
    public WAL apply(Configuration configuration, Long lastSequenceId) {
        Class<?> walClass = configuration.getClass("wal.impl.class", AvroBasedWAL.class);
        WAL wal = (WAL) ReflectionUtils.newInstance(walClass, configuration);
        final String rootPath = new RootPathProvider().apply(configuration);
        wal.init(this.storage, rootPath, new WALFilePathProvider(this.storage, rootPath), lastSequenceId);
        return wal;
    }
}