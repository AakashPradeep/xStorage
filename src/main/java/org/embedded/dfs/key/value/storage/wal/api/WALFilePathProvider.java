package org.embedded.dfs.key.value.storage.wal.api;

import org.embedded.client.api.Storage;
import org.embedded.dfs.key.value.storage.FilePathProvider;
import org.embedded.dfs.key.value.storage.StorageType;

/**
 * Created by aakash on 9/8/17.
 */
public class WALFilePathProvider extends FilePathProvider {

    private static final String WAL_FILE_PREFIX = "wal-file-";

    public WALFilePathProvider(Storage storage, String rootPath) {
        super(storage,rootPath,StorageType.WAL);
    }

    public String getFilePath(long lastSequenceId) {
        String filename = filename(lastSequenceId + 1);
        return getFilePath(filename);
    }

    private String filename(long startSequenceId) {
        return WAL_FILE_PREFIX + System.currentTimeMillis() + "-" + startSequenceId;
    }
}
