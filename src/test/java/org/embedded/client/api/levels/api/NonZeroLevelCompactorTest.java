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
import org.embedded.dfs.key.value.storage.levels.api.impl.NonZeroLevelCompactor;
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
 * Created by aakash on 9/12/17.
 */
public class NonZeroLevelCompactorTest {
    @Test
    public void testLevel1Compactor() throws Exception {
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
            ZeroLevelCompactor zeroLevelCompactor1 = new ZeroLevelCompactor(currentMemstore, rootPath, configuration, workPath);
            zeroLevelCompactor1.call();

            memstoreManager.resetMemstore();
            currentMemstore = memstoreManager.getCurrentMemstore();
            for (int i = 99; i < 200; i++) {
                byte[] value = ("world" + i+"-2").getBytes();
                byte[] key = ("hello" + i).getBytes();
                currentMemstore.put(new KeyValue(Key.from(key), Value.from(value), System.currentTimeMillis()), Operation.PUT);
            }


            ZeroLevelCompactor zeroLevelCompactor2 = new ZeroLevelCompactor(currentMemstore, rootPath, configuration, workPath);
            zeroLevelCompactor2.call();

            HDFSBasedLevelFilesReader hdfsBasedLevelFilesReader = new HDFSBasedLevelFilesReader();
            hdfsBasedLevelFilesReader.setConf(configuration);
            Value value = hdfsBasedLevelFilesReader.get(Key.from("hello90".getBytes()), LevelStorageTypeEnums.LEVEL_0, storage);

            Assert.assertNotNull(value);
            Assert.assertThat(value.getValue(), is(Value.from("world90".getBytes()).getValue()));


            NonZeroLevelCompactor nonZeroLevelCompactor = new NonZeroLevelCompactor(configuration, storage, LevelStorageTypeEnums.LEVEL_1,
                    LevelStorageTypeEnums.LEVEL_0, rootPath, workPath);
            nonZeroLevelCompactor.call();

            HDFSBasedLevelFilesReader hdfsBasedLevelFilesReaderFromLevel1 = new HDFSBasedLevelFilesReader();
            hdfsBasedLevelFilesReaderFromLevel1.setConf(configuration);

            Value valueFromLevel1 = hdfsBasedLevelFilesReaderFromLevel1.get(Key.from("hello90".getBytes()), LevelStorageTypeEnums.LEVEL_1, storage);
            Assert.assertNotNull(valueFromLevel1);
            Assert.assertThat(valueFromLevel1.getValue(), is(Value.from("world90".getBytes()).getValue()));

            Value valueFromLevel2 = hdfsBasedLevelFilesReaderFromLevel1.get(Key.from("hello99".getBytes()), LevelStorageTypeEnums.LEVEL_1, storage);
            Assert.assertNotNull(valueFromLevel2);
            Assert.assertThat(valueFromLevel2.getValue(), is(Value.from("world99-2".getBytes()).getValue()));

        } finally {
            fileSystem.delete(new Path(new RootPathProvider().apply(configuration)), true);
            fileSystem.delete(new Path(new WorkPathProvider().apply(configuration)), true);
        }
    }

}
