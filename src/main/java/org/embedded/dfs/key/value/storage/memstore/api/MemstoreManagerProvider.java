package org.embedded.dfs.key.value.storage.memstore.api;


import org.apache.hadoop.conf.Configuration;
import org.embedded.client.api.Storage;
import org.jboss.netty.util.internal.ConcurrentHashMap;

/**
 * Created by aakash on 9/9/17.
 */
public class MemstoreManagerProvider {
    private final ConcurrentHashMap<Storage, MemstoreManager> storageToMemstoreManagerMap = new ConcurrentHashMap<>();

    private static final MemstoreManagerProvider INSTANCE = new MemstoreManagerProvider();

    private MemstoreManagerProvider() {
    }

    public static synchronized MemstoreManagerProvider getInstance() {
        return INSTANCE;
    }

    public MemstoreManager get(Storage storage, Configuration configuration) {
         if (!this.storageToMemstoreManagerMap.containsKey(storage)) {
            this.storageToMemstoreManagerMap.putIfAbsent(storage, new MemstoreManager(storage, configuration));
        }
        return this.storageToMemstoreManagerMap.get(storage);
    }
}