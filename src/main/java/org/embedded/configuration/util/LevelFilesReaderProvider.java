package org.embedded.configuration.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.embedded.dfs.key.value.storage.levels.api.LevelFilesReader;
import org.embedded.dfs.key.value.storage.levels.api.impl.HDFSBasedLevelFilesReader;

import java.util.function.Function;

/**
 * Created by aakash on 9/11/17.
 */
public class LevelFilesReaderProvider implements Function<Configuration, LevelFilesReader> {
    @Override
    public LevelFilesReader apply(Configuration configuration) {
        Class<?> aClass = configuration.getClass("level.file.reader.impl.class", HDFSBasedLevelFilesReader.class);
        return (LevelFilesReader) ReflectionUtils.newInstance(aClass, configuration);
    }
}
