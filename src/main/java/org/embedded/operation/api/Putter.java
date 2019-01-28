package org.embedded.operation.api;

import org.apache.hadoop.conf.Configuration;
import org.embedded.client.api.Storage;
import org.embedded.configuration.util.RootPathProvider;
import org.embedded.configuration.util.WorkPathProvider;
import org.embedded.dfs.key.value.storage.levels.api.CompactorExecutor;
import org.embedded.dfs.key.value.storage.levels.api.CompactorExecutorProvider;
import org.embedded.dfs.key.value.storage.levels.api.impl.ZeroLevelCompactor;
import org.embedded.dfs.key.value.storage.memstore.api.Memstore;
import org.embedded.dfs.key.value.storage.memstore.api.MemstoreManager;
import org.embedded.dfs.key.value.storage.memstore.api.MemstoreManagerProvider;
import org.embedded.key.value.api.KeyValue;

/**
 * Created by aakash on 9/10/17.
 */
public class Putter {
    private final Storage storage;
    private final MemstoreManager memstoreManager;
    private final String rootPath;
    private final String workPath;
    private final CompactorExecutorProvider compactorExecutorProvider;
    private final RootPathProvider rootPathProvider = new RootPathProvider();
    private final Configuration configuration;

    public Putter(Storage storage, Configuration configuration) {
        this.storage = storage;
        this.configuration = configuration;
        this.rootPath = this.rootPathProvider.apply(configuration);
        this.workPath = new WorkPathProvider().apply(configuration);
        this.memstoreManager = MemstoreManagerProvider.getInstance().get(this.storage, configuration);
        this.compactorExecutorProvider = new CompactorExecutorProvider(configuration);
    }

    public void put(KeyValue keyValue, Operation operation) throws Exception {
        for (int i = 0; i < 10; i++) {
            Memstore currentMemstore = this.memstoreManager.getCurrentMemstore();
            if (currentMemstore.isClosed()) {
                continue;
            }
            currentMemstore.getWal().append(keyValue, operation);
            currentMemstore.put(keyValue, operation);
            if (currentMemstore.shouldNotifyLevelZeroCompactor()) {
                generateLevel0FileForMemstore();
            }
            return;
        }
    }

    synchronized void generateLevel0FileForMemstore() throws Exception {
        Memstore oldMemstore = this.memstoreManager.resetMemstore();
        ZeroLevelCompactor zeroLevelCompactor = new ZeroLevelCompactor(oldMemstore, this.rootPath, this.configuration,
                this.workPath);
        CompactorExecutor compactorExecutor = this.compactorExecutorProvider.get();
        compactorExecutor.submit(zeroLevelCompactor);
    }
}
