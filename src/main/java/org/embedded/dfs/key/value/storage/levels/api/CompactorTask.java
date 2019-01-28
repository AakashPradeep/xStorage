package org.embedded.dfs.key.value.storage.levels.api;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Created by aakash on 9/10/17.
 */
public interface CompactorTask extends Callable<Void> {
    default Void call() throws Exception {
        String level0FilePath = persist();
        cleanUp(level0FilePath);
        return null;
    }

    LevelStorageTypeEnums getTargetLevel();

    void cleanUp(String targetFilePath) throws IOException;

    String persist() throws IOException;
}
