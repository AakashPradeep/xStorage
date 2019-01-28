package org.embedded.dfs.key.value.storage.levels.api;

import org.apache.hadoop.fs.FileStatus;
import org.embedded.client.api.Storage;
import org.embedded.key.value.api.Key;
import org.embedded.key.value.api.Value;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * Created by aakash on 9/10/17.
 */
public interface LevelFilesReader {
    //    Stream<FileStatus> filesList(LevelStorageTypeEnums levelStorageTypeEnums, Storage storage) throws IOException;
    Value get(Key key, LevelStorageTypeEnums initialLevel, Storage storage) throws IOException;
}
