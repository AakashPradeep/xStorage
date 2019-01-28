package org.embedded.operation.api;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.embedded.client.api.Storage;
import org.embedded.configuration.util.*;
import org.embedded.dfs.key.value.storage.levels.api.CompactorExecutorProvider;
import org.embedded.dfs.key.value.storage.levels.api.impl.ZeroLevelCompactor;
import org.embedded.dfs.key.value.storage.memstore.api.Memstore;
import org.embedded.dfs.key.value.storage.wal.api.WAL;
import org.embedded.dfs.key.value.storage.wal.api.WALFilePathProvider;
import org.embedded.dfs.key.value.storage.wal.api.WALReaderSupplier;
import org.embedded.dfs.key.value.storage.wal.api.avro.impl.AvroWalReaderSupplier;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;

/**
 * Created by aakash on 9/10/17.
 */
public class StorageBootUpManager implements Callable<Boolean> {
    private final Storage storage;
    private final Configuration configuration;
    private final CompactorExecutorProvider compactorExecutorProvider;
    private final WALReaderSupplier walReaderSupplier;
    private final String rootPath;
    private final String workPath;
    private Cache<Path, Memstore> cache;

    public StorageBootUpManager(Storage storage, Configuration configuration) {
        this.storage = storage;
        this.configuration = configuration;
        this.compactorExecutorProvider = new CompactorExecutorProvider(this.configuration);
        this.rootPath = new RootPathProvider().apply(configuration);
        this.workPath = new WorkPathProvider().apply(configuration);
        Class<?> walReaderSupplierClass = this.configuration.getClass("wal.reader.supplier.impl.class", AvroWalReaderSupplier.class);
        this.walReaderSupplier = (WALReaderSupplier) ReflectionUtils.newInstance(walReaderSupplierClass, this.configuration);
    }

    public Collection<Memstore> getCache() {
        if (this.cache == null) {
            return Collections.EMPTY_LIST;
        }
        return this.cache.asMap().values();
    }

    @Override
    public Boolean call() throws Exception {

        WALFilePathProvider walFilePathProvider = new WALFilePathProvider(this.storage,this.rootPath);

        String walStorageDir = walFilePathProvider.getStorageDir();
        FileSystem fileSystem = FileSystem.get(this.configuration);
        Path walStorageDirPath = new Path(walStorageDir);
        if (fileSystem.exists(walStorageDirPath)) {
            FileStatus[] walFilePaths = fileSystem.listStatus(walStorageDirPath);
            this.cache = CacheBuilder.newBuilder().maximumSize(walFilePaths != null ? walFilePaths.length : 1)
                    .softValues()
                    .build();

            if (walFilePaths != null) {
                for (FileStatus walFilePath : walFilePaths) {
                    try (WAL.Reader<WAL.Entry> reader = this.walReaderSupplier.getReader(walFilePath.getPath().toString())) {
                        Memstore memstore = new MemstoreProvider().apply(this.configuration);
                        long lastSequenceId = 0;
                        for (WAL.Entry entry : reader) {
                            memstore.put(entry.getKeyValue(), entry.getOperation());
                            lastSequenceId = entry.getSequenceId();
                        }
                        if (memstore.count() > 0) {
                            this.cache.put(walFilePath.getPath(), memstore);
                            WAL wal = new WALProvider(this.storage).apply(this.configuration, lastSequenceId);
                            memstore.init(this.storage, wal, new MemstoreMaxSizeLimitProvider().apply(this.configuration));
                            LevelCompactorWrapper compactorWrapper = new LevelCompactorWrapper(memstore, this.rootPath, this.configuration,
                                    this.workPath, this.cache, walFilePath.getPath());
                            this.compactorExecutorProvider.get().submit(compactorWrapper);
                        }
                    }
                }
            }
        }
        return Boolean.TRUE;
    }

    public static class LevelCompactorWrapper extends ZeroLevelCompactor {
        private final Cache<Path, Memstore> cache;
        private final Path walPath;

        public LevelCompactorWrapper(Memstore memstore, String rootPath, Configuration configuration, String workPath, Cache<Path, Memstore> cache, Path walPath) {
            super(memstore, rootPath, configuration, workPath);
            this.cache = cache;
            this.walPath = walPath;
        }

        @Override
        public void cleanUp(String targetFilePath) throws IOException {
            this.sourcePathsToBeDeleted.add(this.walPath);
            super.cleanUp(targetFilePath);
            this.cache.invalidateAll(super.sourcePathsToBeDeleted);
        }
    }


}
