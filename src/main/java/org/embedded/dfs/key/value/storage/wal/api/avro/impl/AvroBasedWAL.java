package org.embedded.dfs.key.value.storage.wal.api.avro.impl;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.embedded.client.api.Storage;
import org.embedded.dfs.key.value.storage.wal.api.WAL;
import org.embedded.dfs.key.value.storage.wal.api.WALFilePathProvider;
import org.embedded.key.value.api.KeyValue;
import org.embedded.operation.api.Operation;

import java.io.IOException;

/**
 * Created by aakash on 9/8/17.
 */
public class AvroBasedWAL extends Configured implements WAL {
    private Storage storage;
    private Path rootPath;
    private Path finalWalPath;
    private long currentSequenceId;
    private Writer<Entry> writer;


    public void init(Storage storage, String rootPath, WALFilePathProvider filePathProvider, long lastSequenceId) {
        this.storage = storage;
        this.rootPath = new Path(rootPath);
        this.currentSequenceId = lastSequenceId + 1;
        this.finalWalPath = new Path(filePathProvider.getFilePath(this.currentSequenceId));
    }

    @Override
    public Storage getStorage() {
        return this.storage;
    }

    @Override
    public String getRootPath() {
        return this.rootPath.toString();
    }

    @Override
    public String walFilePath() {
        return this.finalWalPath.toString();
    }

    @Override
    public long getCurrentSequenceId() {
        return this.currentSequenceId;
    }

    @Override
    public boolean append(KeyValue keyValue, Operation operation) throws IOException {
        boolean write = getWriter().write(new Entry(this.currentSequenceId, keyValue, operation));
        this.currentSequenceId += 1;
        return write;
    }

    private synchronized Writer<Entry> getWriter() throws IOException {
        if (writer == null) {
            writer = ReflectionUtils.newInstance(AvroWalWriterSupplier.class, getConf()).getWriter(this.walFilePath());
        }
        return writer;
    }


    @Override
    public void close() throws Exception {
        if (writer != null) {
            this.writer.close();
        }
    }
}
