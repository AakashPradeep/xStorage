package org.embedded.dfs.key.value.storage.levels.api;

/**
 * Created by aakash on 9/10/17.
 */
public interface UpperLimitDefinerForLevel {
    boolean shouldCompactToNextLevel(LevelStorageTypeEnums levelStorageTypeEnums, long sizeInBytes, long numberOfFiles);

    default LevelStorageTypeEnums getNextLevel(LevelStorageTypeEnums current) {
        switch (current) {
            case LEVEL_0:
                return LevelStorageTypeEnums.LEVEL_1;
            case LEVEL_1:
                return LevelStorageTypeEnums.LEVEL_2;
            case LEVEL_2:
                return LevelStorageTypeEnums.LEVEL_3;
            case LEVEL_3:
                return LevelStorageTypeEnums.LEVEL_4;
            case LEVEL_4:
                return LevelStorageTypeEnums.LEVEL_5;
            case LEVEL_5:
                return LevelStorageTypeEnums.LEVEL_6;
            case LEVEL_6:
                return LevelStorageTypeEnums.LEVEL_7;
        }
        return null;
    }
}
