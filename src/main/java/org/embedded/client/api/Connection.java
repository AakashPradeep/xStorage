package org.embedded.client.api;

import org.apache.hadoop.conf.Configuration;
import org.embedded.client.api.exception.StorageBootUpException;
import org.embedded.configuration.util.LevelFilesReaderProvider;
import org.embedded.dfs.key.value.storage.levels.api.CompactorExecutorProvider;
import org.embedded.dfs.key.value.storage.memstore.api.MemstoreManagerProvider;
import org.embedded.key.value.api.Key;
import org.embedded.key.value.api.KeyValue;
import org.embedded.key.value.api.Value;
import org.embedded.operation.api.Getter;
import org.embedded.operation.api.Operation;
import org.embedded.operation.api.Putter;
import org.embedded.operation.api.StorageBootUpManager;

import java.io.IOException;

/**
 * Created by aakash on 9/10/17.
 */
public class Connection implements AutoCloseable {
    private final Storage storage;
    private final Configuration configuration;
    private final StorageBootUpManager storageBootUpManager;

    Connection(Storage storage, Configuration configuration) {
        this.storage = storage;
        this.configuration = configuration;
        this.storageBootUpManager = new StorageBootUpManager(this.storage, this.configuration);
        try {
            this.storageBootUpManager.call();
        } catch (Exception e) {
            throw new StorageBootUpException(e);
        }
    }

    public void put(byte[] key, byte[] value) throws Exception {
        new Putter(this.storage, this.configuration).put(new KeyValue(Key.from(key),
                Value.from(value), System.currentTimeMillis()), Operation.PUT);
    }

    public byte[] get(byte[] key) throws IOException {
        Value value = new Getter(this.storage, this.configuration, this.storageBootUpManager,
                new LevelFilesReaderProvider()).get(Key.from(key));
        return value != null ? value.getValue() : null;
    }

    public void delete(byte[] key) throws Exception {
        new Putter(this.storage, this.configuration).put(new KeyValue(Key.from(key),
                Value.ofNullable(), System.currentTimeMillis()), Operation.DELETE);
    }

    @Override
    public void close() throws Exception {
        new CompactorExecutorProvider(this.configuration).get().shutdown();
        MemstoreManagerProvider.getInstance().get(this.storage, this.configuration).close();
    }
}
