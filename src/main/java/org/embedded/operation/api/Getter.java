package org.embedded.operation.api;

import org.apache.hadoop.conf.Configuration;
import org.embedded.client.api.Storage;
import org.embedded.configuration.util.LevelFilesReaderProvider;
import org.embedded.dfs.key.value.storage.levels.api.LevelFilesReader;
import org.embedded.dfs.key.value.storage.levels.api.LevelStorageTypeEnums;
import org.embedded.dfs.key.value.storage.memstore.api.Memstore;
import org.embedded.dfs.key.value.storage.memstore.api.MemstoreManager;
import org.embedded.dfs.key.value.storage.memstore.api.MemstoreManagerProvider;
import org.embedded.dfs.key.value.storage.memstore.api.impl.SkipListBasedMemstore;
import org.embedded.key.value.api.Key;
import org.embedded.key.value.api.Value;

import java.io.IOException;
import java.util.Collection;

/**
 * Created by aakash on 9/10/17.
 */
public class Getter {
    private final Storage storage;
    private final MemstoreManager memstoreManager;
    private final Configuration configuration;
    private final StorageBootUpManager storageBootUpManager;
    private final LevelFilesReader levelFilesReader;

    public Getter(Storage storage, Configuration configuration, StorageBootUpManager storageBootUpManager, LevelFilesReaderProvider levelFilesReaderProvider) {
        this.storage = storage;
        this.memstoreManager = MemstoreManagerProvider.getInstance().get(this.storage, configuration);
        this.configuration = configuration;
        this.storageBootUpManager = storageBootUpManager;
        this.levelFilesReader = levelFilesReaderProvider.apply(configuration);
    }

    public Value get(Key key) throws IOException {
        Memstore currentMemstore = this.memstoreManager.getCurrentMemstore();
        Value value = getValueFromMemstore(key, currentMemstore);
        if (value != null) return value;


        Value valueFromOldMemstore = getValueFromMemstoreCollections(key, this.memstoreManager.visitCachedMemstore());
        if (valueFromOldMemstore != null) return valueFromOldMemstore;


        Value valueFromBackUpMemstore = getValueFromMemstoreCollections(key, this.storageBootUpManager.getCache());
        if (valueFromBackUpMemstore != null) return valueFromBackUpMemstore;

        //value to be read from different level and caching of level file
        return levelFilesReader.get(key, LevelStorageTypeEnums.LEVEL_0, this.storage);
    }

    Value getValueFromMemstoreCollections(Key key, Collection<Memstore> storageBootupManagerCache) {
        if (storageBootupManagerCache != null) {
            for (Memstore memstore : storageBootupManagerCache) {
                Value valueFromBackUpMemstore = getValueFromMemstore(key, memstore);
                if (valueFromBackUpMemstore != null) return valueFromBackUpMemstore;
            }
        }
        return null;
    }

    Value getValueFromMemstore(Key key, Memstore currentMemstore) {
        Memstore.Entry<SkipListBasedMemstore.ValueWithTimestamp> valueWithTimestampEntryFromCurrentStore = currentMemstore.get(key);
        if (valueWithTimestampEntryFromCurrentStore != null) {
            if (valueWithTimestampEntryFromCurrentStore.getOperation() == Operation.DELETE) {
                return Value.ofNullable();
            }
            return valueWithTimestampEntryFromCurrentStore.getEntity().getValue();
        }
        return null;
    }
}
