package org.embedded.dfs.key.value.storage.memstore.api;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import org.apache.hadoop.conf.Configuration;
import org.embedded.client.api.Storage;
import org.embedded.configuration.util.MemstoreMaxMemberLimitProvider;
import org.embedded.configuration.util.MemstoreMaxSizeLimitProvider;
import org.embedded.configuration.util.MemstoreProvider;
import org.embedded.configuration.util.WALProvider;
import org.embedded.dfs.key.value.storage.wal.api.WAL;
import org.embedded.dfs.key.value.storage.wal.api.WALFilePathProvider;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by aakash on 9/8/17.
 */
public class MemstoreManager implements AutoCloseable {
    private final Storage storage;
    private final Cache<Integer, Memstore> cache;
    private final Configuration configuration;
    private final AtomicReference<Memstore> currentMemstore;
    private final long memstoreMaxSizeLimit;
    private final int memstoreMaxMemberLimit;
    private final WALFilePathProvider walFilePathProvider;
    private final String rootPath;


    MemstoreManager(Storage storage, Configuration configuration) {
        this.storage = storage;
        this.configuration = configuration;
        Memstore memstore = newMemstore();
        this.currentMemstore = new AtomicReference<>(memstore);

        memstoreMaxMemberLimit = new MemstoreMaxMemberLimitProvider().apply(configuration);
        memstoreMaxSizeLimit = new MemstoreMaxSizeLimitProvider().apply(configuration);
        this.cache = CacheBuilder.newBuilder() //.maximumSize(memstoreMaxMemberLimit)
                .maximumWeight(memstoreMaxMemberLimit * memstoreMaxSizeLimit)
                .weigher((Weigher<Integer, Memstore>) (key, value) -> new Long(value.size()).intValue())
                .softValues()
                .build();

        this.rootPath = configuration.get("org.embedded.dfs.key.value.storage.root.path", "org/embedded/dfs/key/value/storage/root/path");
        this.walFilePathProvider = new WALFilePathProvider(this.storage, this.rootPath);
        WAL wal = getWAL(configuration, 0);
        memstore.init(this.storage, wal, memstoreMaxSizeLimit);
    }

    public Collection<Memstore> visitCachedMemstore() {
        return this.cache.asMap().values();
    }

    public Memstore getCurrentMemstore() {
        return this.currentMemstore.get();
    }

    public synchronized Memstore resetMemstore() throws Exception {
        Memstore memstore = newMemstore();
        Memstore oldMemstore = this.currentMemstore.getAndSet(memstore);
        long lastSequenceId = oldMemstore.getWal().getCurrentSequenceId();
        memstore.init(this.storage, getWAL(this.configuration, lastSequenceId), this.memstoreMaxSizeLimit);

        if (oldMemstore.size() > 0) {
            int numberOfMemstoreInCache = new Long(this.cache.size()).intValue();
            this.cache.put(new Integer(numberOfMemstoreInCache + 1), oldMemstore);
        }
        oldMemstore.close();
        return oldMemstore;
    }

    private WAL getWAL(Configuration configuration, long lastSequenceId) {
        return new WALProvider(this.storage).apply(configuration, lastSequenceId);
    }

    private Memstore newMemstore() {
        return new MemstoreProvider().apply(this.configuration);
    }


    @Override
    public void close() throws Exception {
        this.getCurrentMemstore().getWal().close();
        this.cache.invalidateAll();
    }
}

