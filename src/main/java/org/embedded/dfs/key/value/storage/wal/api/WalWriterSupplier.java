package org.embedded.dfs.key.value.storage.wal.api;

import org.apache.hadoop.conf.Configurable;

import java.io.IOException;

/**
 * Created by aakash on 9/8/17.
 */
public interface WalWriterSupplier extends Configurable {
    <T extends WAL.Entry> WAL.Writer<T> getWriter(String filePath) throws IOException;
}
