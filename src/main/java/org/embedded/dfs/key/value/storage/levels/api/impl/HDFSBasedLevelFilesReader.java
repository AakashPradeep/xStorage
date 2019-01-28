package org.embedded.dfs.key.value.storage.levels.api.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.BytesWritable;
import org.embedded.client.api.Storage;
import org.embedded.configuration.util.RootPathProvider;
import org.embedded.configuration.util.UpperLimitDefinerForLevelProvider;
import org.embedded.dfs.key.value.storage.FilePathProvider;
import org.embedded.dfs.key.value.storage.levels.api.LevelFileValueWritable;
import org.embedded.dfs.key.value.storage.levels.api.LevelFilesReader;
import org.embedded.dfs.key.value.storage.levels.api.LevelStorageTypeEnums;
import org.embedded.dfs.key.value.storage.levels.api.UpperLimitDefinerForLevel;
import org.embedded.key.value.api.Key;
import org.embedded.key.value.api.Value;
import org.embedded.operation.api.Operation;

import java.io.IOException;

/**
 * Created by aakash on 9/10/17.
 */
public class HDFSBasedLevelFilesReader extends Configured implements LevelFilesReader {
    FileStatus[] filesList(LevelStorageTypeEnums levelStorageTypeEnums, Storage storage)
            throws IOException {
        Configuration conf = getConf();
        FileSystem fileSystem = FileSystem.get(conf);
        FilePathProvider filePathProvider = new FilePathProvider(storage, new RootPathProvider().apply(conf),
                levelStorageTypeEnums.getStorageType());
        Path filePath = new Path(filePathProvider.getStorageDir());
        if (!fileSystem.exists(filePath)) {
            return null;
        }
        return fileSystem.listStatus(filePath);
    }


    public Value get(Key key, LevelStorageTypeEnums initialLevel, Storage storage) throws IOException {
        UpperLimitDefinerForLevel upperLimitDefinerForLevel = new UpperLimitDefinerForLevelProvider().apply(getConf());
        LevelStorageTypeEnums startLevel = initialLevel;
        for (; startLevel != null; startLevel = upperLimitDefinerForLevel.getNextLevel(startLevel)) {
            FileStatus[] fileStatuses = filesList(startLevel, storage);
            if (fileStatuses != null) {
                for (FileStatus fileStatus : fileStatuses) {
                    try (BloomMapFile.Reader reader = new BloomMapFile.Reader(fileStatus.getPath(), getConf())) {
                        LevelFileValueWritable levelFileValueWritable = (LevelFileValueWritable) reader.get(new BytesWritable(key.getValue()), new LevelFileValueWritable());
                        if (levelFileValueWritable != null) {
                            if (levelFileValueWritable.getOperation() == Operation.DELETE) {
                                return Value.ofNullable();
                            } else {
                                return levelFileValueWritable.getValue();
                            }
                        }
                    }
                }
            }
        }

        return Value.ofNullable();
    }

}
