package org.embedded.client.api.levels.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.embedded.client.api.Namespace;
import org.embedded.client.api.Storage;
import org.embedded.client.api.StorageName;
import org.embedded.configuration.util.RootPathProvider;
import org.embedded.configuration.util.WorkPathProvider;
import org.embedded.dfs.key.value.storage.levels.api.LevelStorageTypeEnums;
import org.embedded.dfs.key.value.storage.levels.api.impl.HDFSBasedLevelFilesReader;
import org.embedded.dfs.key.value.storage.levels.api.impl.ZeroLevelCompactor;
import org.embedded.dfs.key.value.storage.memstore.api.Memstore;
import org.embedded.dfs.key.value.storage.memstore.api.MemstoreManager;
import org.embedded.dfs.key.value.storage.memstore.api.MemstoreManagerProvider;
import org.embedded.key.value.api.Key;
import org.embedded.key.value.api.KeyValue;
import org.embedded.key.value.api.Value;
import org.embedded.operation.api.Operation;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;

/**
 * Created by aakash on 9/11/17.
 */
public class ZeroLevelCompactorTest {
    @Test
    public void testLevel0Compactor() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("root.path", "/tmp/root_1/data/" + this.getClass().getSimpleName());
        configuration.set("work.path", "/tmp/work_1/data/" + this.getClass().getSimpleName());
        FileSystem fileSystem = FileSystem.get(configuration);
        Storage storage = new Storage(Namespace.from("com.aakash"), StorageName.from(this.getClass().getSimpleName()));
        try {
            MemstoreManager memstoreManager = MemstoreManagerProvider.getInstance().get(storage, configuration);
            Memstore currentMemstore = memstoreManager.getCurrentMemstore();
            for (int i = 0; i < 100; i++) {
                byte[] value = ("world" + i).getBytes();
                byte[] key = ("hello" + i).getBytes();
                currentMemstore.put(new KeyValue(Key.from(key), Value.from(value), System.currentTimeMillis()), Operation.PUT);
            }

            String workPath = new WorkPathProvider().apply(configuration);
            String rootPath = new RootPathProvider().apply(configuration);
            ZeroLevelCompactor zeroLevelCompactor = new ZeroLevelCompactor(currentMemstore, rootPath, configuration, workPath);
            zeroLevelCompactor.call();

            HDFSBasedLevelFilesReader hdfsBasedLevelFilesReader = new HDFSBasedLevelFilesReader();
            hdfsBasedLevelFilesReader.setConf(configuration);
            Value value = hdfsBasedLevelFilesReader.get(Key.from("hello90".getBytes()), LevelStorageTypeEnums.LEVEL_0, storage);

            Assert.assertNotNull(value);
            Assert.assertThat(value.getValue(), is(Value.from("world90".getBytes()).getValue()));
        } finally {
            fileSystem.delete(new Path(new RootPathProvider().apply(configuration)), true);
            fileSystem.delete(new Path(new WorkPathProvider().apply(configuration)), true);
        }
    }

}
