package org.embedded.dfs.key.value.storage.levels.api.impl;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.BytesWritable;
import org.embedded.dfs.key.value.storage.FilePathProvider;
import org.embedded.dfs.key.value.storage.levels.api.LevelFileValueWritable;
import org.embedded.dfs.key.value.storage.levels.api.LevelStorageTypeEnums;
import org.embedded.dfs.key.value.storage.memstore.api.Memstore;
import org.embedded.key.value.api.KeyValue;

import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;

/**
 * Created by aakash on 9/9/17.
 */
public class ZeroLevelCompactor extends AbstractCompactorTask {
    private final Memstore memstore;
    private final Configuration configuration;

    public ZeroLevelCompactor(Memstore memstore, String rootPath, Configuration configuration, String workPath) {
        super(configuration, LevelStorageTypeEnums.LEVEL_0, rootPath, workPath, memstore.getStorage());
        this.configuration = configuration;
        this.memstore = memstore;
    }


    public String persist() throws IOException {
        FileSystem fileSystem = FileSystem.get(this.configuration);
        FilePathProvider workingDirPathProvider = new FilePathProvider(this.storage, this.workPath,
                this.targetLevelStorageTypeEnum.getStorageType());
        fileSystem.mkdirs(new Path(workingDirPathProvider.getStorageDir()));
        String workFilename = this.targetLevelStorageTypeEnum.toString() + "-" + System.currentTimeMillis() + "-" + UUID.randomUUID().toString();
        String workFilePath = workingDirPathProvider.getFilePath(workFilename);

        try (final BloomMapFile.Writer writer = getBloomFileWriter(workFilePath)) {
            Iterator<Memstore.Entry<KeyValue>> iterator = this.memstore.stream().iterator();
            while (iterator.hasNext()) {
                Memstore.Entry<KeyValue> entry = iterator.next();
                KeyValue keyValue = entry.getEntity();
                writer.append(new BytesWritable(keyValue.key().getValue()), new LevelFileValueWritable(
                        keyValue, entry.getOperation()
                ));
            }
        }
        super.sourcePathsToBeDeleted = Lists.newArrayList(new Path(this.memstore.getWal().walFilePath()));
        return moveWorkFilePathToDestinationFilePath(fileSystem, workFilename, workFilePath);
    }
}
