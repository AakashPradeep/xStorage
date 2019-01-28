package org.embedded.dfs.key.value.storage.levels.api.transaction;

import org.embedded.dfs.key.value.storage.levels.api.LevelStorageTypeEnums;

/**
 * Created by aakash on 9/27/17.
 */
public interface CompactionTransactionHistory {

    void start(LevelStorageTypeEnums levelStorageTypeEnums);
    void markInProgress(LevelStorageTypeEnums levelStorageTypeEnums,
                       TransactionWork transactionWork);
    void markcommitted(LevelStorageTypeEnums levelStorageTypeEnums,
                       TransactionWork transactionWork);

    interface TransactionWork{
        public enum Status {
            START, IN_PROGRESS, COMMIT
        }
        String name();
        Status status();
    }
}
