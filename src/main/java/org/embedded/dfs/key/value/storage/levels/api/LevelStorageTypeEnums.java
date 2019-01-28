package org.embedded.dfs.key.value.storage.levels.api;

import org.embedded.dfs.key.value.storage.StorageType;

/**
 * Created by aakash on 9/10/17.
 */
public enum LevelStorageTypeEnums {
    LEVEL_0(0),
    LEVEL_1(1),
    LEVEL_2(2),
    LEVEL_3(3),
    LEVEL_4(4),
    LEVEL_5(5),
    LEVEL_6(6),
    LEVEL_7(7);

    private final StorageType storageType;
    private final int level;

    LevelStorageTypeEnums(int level) {
        this.storageType = new StorageType("Level-" + level);
        this.level = level;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public int getLevel() {
        return level;
    }
}
