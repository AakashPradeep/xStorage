package org.embedded.dfs.key.value.storage.levels.api;

import org.apache.hadoop.conf.Configuration;


public class CompactorExecutorProvider {
    private final Configuration configuration;

    public CompactorExecutorProvider(Configuration configuration) {
        this.configuration = configuration;
    }

    public CompactorExecutor get() {
        return lazilyInitializedCompactorExecutor();
    }

    CompactorExecutor lazilyInitializedCompactorExecutor() {
        CompactorExecutor compactorExecutor = CompactorExecutor.getInstance();
        compactorExecutor.setConf(this.configuration);
        compactorExecutor.init();
        return compactorExecutor;
    }
}