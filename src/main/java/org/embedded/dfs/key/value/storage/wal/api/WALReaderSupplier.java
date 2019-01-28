package org.embedded.dfs.key.value.storage.wal.api;

import org.apache.hadoop.conf.Configurable;

import java.io.IOException;

/**
 * Created by aakash on 9/8/17.
 */
public interface WALReaderSupplier extends Configurable {
    <T extends WAL.Entry> WAL.Reader<T> getReader(String filePath) throws IOException;
}
