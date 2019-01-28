package org.embedded.dfs.key.value.storage.levels.api.impl;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.embedded.client.api.Storage;
import org.embedded.dfs.key.value.storage.FilePathProvider;
import org.embedded.dfs.key.value.storage.levels.api.LevelFileValueWritable;
import org.embedded.dfs.key.value.storage.levels.api.LevelStorageTypeEnums;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Created by aakash on 9/9/17.
 */
public class NonZeroLevelCompactor extends AbstractCompactorTask {
    private final LevelStorageTypeEnums sourceLevelStorageTypeEnum;

    public NonZeroLevelCompactor(Configuration configuration, Storage storage, LevelStorageTypeEnums targetStorageType,
                                 LevelStorageTypeEnums sourceLevelStorageTypeEnum, String rootPath, String workDir) {
        super(configuration, targetStorageType, rootPath, workDir, storage);
        this.sourceLevelStorageTypeEnum = sourceLevelStorageTypeEnum;
    }


    /*
    Naive implementation will go out of memory for Level 3 and above
     */
    public String persist() throws IOException {

        FilePathProvider sourceFilePathProvider = new FilePathProvider(this.storage, this.rootPath,
                this.sourceLevelStorageTypeEnum.getStorageType());
        String sourceStoragePath = sourceFilePathProvider.getStorageDir();
        FileSystem fileSystem = FileSystem.get(this.configuration);
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(sourceStoragePath));
        FilePathProvider workingDirPathProvider = new FilePathProvider(this.storage, this.workPath,
                this.targetLevelStorageTypeEnum.getStorageType());
        fileSystem.mkdirs(new Path(workingDirPathProvider.getStorageDir()));
        String filename = this.targetLevelStorageTypeEnum.toString() + "-" + System.currentTimeMillis() + "-" + UUID.randomUUID().toString();

        String workFilePath = workingDirPathProvider.getFilePath(filename);
        List<Path> paths = Arrays.stream(fileStatuses).map(f -> f.getPath()).collect(Collectors.toList());
        Path [] paths1 = new Path[paths.size()];
        paths.toArray(paths1);
        BloomMapFilesMerger bloomMapFilesMerger = new BloomMapFilesMerger(this.configuration);
        bloomMapFilesMerger.merge(paths1,false,new Path(workFilePath));
        this.sourcePathsToBeDeleted = paths;
        String targetFilePath = moveWorkFilePathToDestinationFilePath(fileSystem, filename, workFilePath);
        return targetFilePath;
    }
}
