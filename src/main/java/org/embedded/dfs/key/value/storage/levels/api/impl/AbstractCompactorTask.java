package org.embedded.dfs.key.value.storage.levels.api.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.embedded.client.api.Storage;
import org.embedded.configuration.util.LevelFileCompressionCodecProvider;
import org.embedded.configuration.util.UpperLimitDefinerForLevelProvider;
import org.embedded.dfs.key.value.storage.FilePathProvider;
import org.embedded.dfs.key.value.storage.levels.api.*;

import java.io.IOException;
import java.util.Collection;

/**
 * Created by aakash on 9/10/17.
 */
public abstract class AbstractCompactorTask implements CompactorTask {
    protected final Configuration configuration;
    protected final LevelStorageTypeEnums targetLevelStorageTypeEnum;
    protected final String rootPath;
    protected final String workPath;
    protected final Storage storage;
    private final UpperLimitDefinerForLevel upperLimitDefinerForLevel;
    protected Collection<Path> sourcePathsToBeDeleted;

    protected AbstractCompactorTask(Configuration configuration, LevelStorageTypeEnums targetLevelStorageTypeEnum,
                                    String rootPath, String workPath, Storage storage) {
        this.configuration = configuration;
        this.targetLevelStorageTypeEnum = targetLevelStorageTypeEnum;
        this.rootPath = rootPath;
        this.workPath = workPath;
        this.storage = storage;
        this.upperLimitDefinerForLevel = new UpperLimitDefinerForLevelProvider().apply(configuration);
    }


    protected String moveWorkFilePathToDestinationFilePath(FileSystem fileSystem, String filename, String workFilePath)
            throws IOException {
        FilePathProvider targetDirPathProvider = new FilePathProvider(this.storage, this.rootPath,
                this.targetLevelStorageTypeEnum.getStorageType());
        String targetDir = targetDirPathProvider.getStorageDir();
        fileSystem.mkdirs(new Path(targetDir));
        String targetFilePath = targetDirPathProvider.getFilePath(filename);
        Path src = new Path(workFilePath);
        Path dst = new Path(targetFilePath);
        if (fileSystem.rename(src, dst)) {
            return targetFilePath;
        } else {
            if (!FileUtil.copy(fileSystem, src, fileSystem, dst, true, configuration)) {
                throw new RuntimeException("file rename failed");
            }
            return targetFilePath;
        }

    }

    protected BloomMapFile.Writer getBloomFileWriter(String workingFilePath) throws IOException {
        LevelFileCompressionCodecProvider levelFileCompressionCodecProvider = new LevelFileCompressionCodecProvider();
        CompressionCodec compressionCodec = levelFileCompressionCodecProvider.apply(this.configuration);

        if (levelFileCompressionCodecProvider.isCompressionEnabled(configuration)) {
            return new BloomMapFile.Writer(this.configuration, new Path(workingFilePath),
                    BloomMapFile.Writer.keyClass(BytesWritable.class),
                    BloomMapFile.Writer.valueClass(LevelFileValueWritable.class),
                    BloomMapFile.Writer.compression(SequenceFile.CompressionType.BLOCK, compressionCodec));
        }

        return new BloomMapFile.Writer(this.configuration, new Path(workingFilePath),
                BloomMapFile.Writer.keyClass(BytesWritable.class),
                BloomMapFile.Writer.valueClass(LevelFileValueWritable.class));
    }

    public void cleanUp(String targetFilePath) throws IOException {
        FileSystem fileSystem = FileSystem.get(this.configuration);
        for (Path path : this.sourcePathsToBeDeleted) {
            fileSystem.delete(path, true);
        }
        ContentSummary contentSummary = fileSystem.getContentSummary(new Path(targetFilePath).getParent());
        long directoryCount = contentSummary.getDirectoryCount();
        long spaceConsumed = contentSummary.getSpaceConsumed();
        if (this.upperLimitDefinerForLevel.shouldCompactToNextLevel(this.targetLevelStorageTypeEnum, spaceConsumed,
                directoryCount)) {
            LevelStorageTypeEnums newTargetLevel = this.upperLimitDefinerForLevel.getNextLevel(this.getTargetLevel());
            if (newTargetLevel == null) {
                return;
            }
            NonZeroLevelCompactor nonZeroLevelCompactor = new NonZeroLevelCompactor(this.configuration, this.storage,
                    newTargetLevel, this.targetLevelStorageTypeEnum, this.rootPath
                    , this.workPath);

            CompactorExecutor.getInstance().submit(nonZeroLevelCompactor);
        }
    }

    @Override
    public LevelStorageTypeEnums getTargetLevel() {
        return this.targetLevelStorageTypeEnum;
    }
}
