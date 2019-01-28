package org.embedded.dfs.key.value.storage.levels.api;

/**
 * Created by aakash on 9/10/17.
 */
public class DefaultUpperLimitDefinerForLevel implements UpperLimitDefinerForLevel {
    public static final long _100MB = 50 * 1024 * 1024;

    /*
    https://r.va.gg/presentations/sf.nodebase.meetup/#/3
     */
    @Override
    public boolean shouldCompactToNextLevel(LevelStorageTypeEnums levelStorageTypeEnums, long sizeInBytes, long numberOfFiles) {
        switch (levelStorageTypeEnums) {
            case LEVEL_0:
                return numberOfFiles > 4;
            case LEVEL_1:
                return sizeInBytes > _100MB;
            case LEVEL_2:
                return sizeInBytes > _100MB * 10; //1GB
            case LEVEL_3:
                return sizeInBytes > _100MB * 10 * 10; //10GB
            case LEVEL_4:
                return sizeInBytes > _100MB * 10 * 10 * 10; //100GB
            case LEVEL_5:
                return sizeInBytes > _100MB * 10 * 10 * 10 * 10; //1TB -- This should be enough for an embedded system.
            case LEVEL_6:
                return sizeInBytes > _100MB * 10 * 10 * 10 * 10 * 10; //10TB
            case LEVEL_7:
                return sizeInBytes > _100MB * 10 * 10 * 10 * 10 * 10 * 10; //100TB
        }
        return false;
    }
}
