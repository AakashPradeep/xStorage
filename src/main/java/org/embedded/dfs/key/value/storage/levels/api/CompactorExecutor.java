package org.embedded.dfs.key.value.storage.levels.api;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by aakash on 9/10/17.
 */
public class CompactorExecutor extends Configured {
    static final CompactorExecutor INSTANCE = new CompactorExecutor();
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final Map<LevelStorageTypeEnums,ExecutorService> levelToExecutorServiceMap =
            new ConcurrentHashMap<>(LevelStorageTypeEnums.values().length);

    private CompactorExecutor() {

    }

    public static CompactorExecutor getInstance() {
        return INSTANCE;
    }

    public void init() {
        if (!this.initialized.get()) {
            Configuration configuration = getConf();
            int threadPoolSize = configuration.getInt("level.zero.compactor.thread.pool.size", 5);
            ExecutorService levelZeroExecutorService = Executors.newFixedThreadPool(threadPoolSize, getThreadFactory());
            this.levelToExecutorServiceMap.put(LevelStorageTypeEnums.LEVEL_0, levelZeroExecutorService);
            this.initialized.compareAndSet(false, true);
        }
    }

    ThreadFactory getThreadFactory() {
        return new SimpleThreadFactory();
    }

    public Future<Void> submit(CompactorTask compactorTask) {
        if (!this.initialized.get()) {
            throw new IllegalStateException("please call init() first");
        }
        return getExecutorService(compactorTask.getTargetLevel()).submit(compactorTask);
    }

    private synchronized ExecutorService getExecutorService(LevelStorageTypeEnums levelStorageTypeEnums){
        if(!this.levelToExecutorServiceMap.containsKey(levelStorageTypeEnums)){
            this.levelToExecutorServiceMap.putIfAbsent(levelStorageTypeEnums,
                    Executors.newSingleThreadExecutor(getThreadFactory()));
        }
        return this.levelToExecutorServiceMap.get(levelStorageTypeEnums);
    }

    public void shutdown() {
        if (initialized.get()) {
            levelToExecutorServiceMap.forEach((l,e) ->
            e.shutdown());
        }
    }

    public List<Runnable> shutdownNow() {
        if (initialized.get()) {
            final List<Runnable> runnables = Lists.newLinkedList();
            levelToExecutorServiceMap.forEach((l,e) ->
                    runnables.addAll(shutdownNow()));
            return runnables;
        }
        return Collections.EMPTY_LIST;
    }

    public static class SimpleThreadFactory implements ThreadFactory {

        private ThreadFactory threadFactory = Executors.defaultThreadFactory();

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = threadFactory.newThread(r);
            String threadName = thread.getName();
            thread.setName(this.getClass().getName() + "-" + r.getClass().getName() + "-" + threadName);
            thread.setDaemon(true);
            thread.setPriority(Thread.MAX_PRIORITY);
            return thread;
        }
    }
}
