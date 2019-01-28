package org.embedded.dfs.key.value.storage.wal.api.avro.impl;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.embedded.dfs.key.value.storage.wal.api.WAL;
import org.embedded.dfs.key.value.storage.wal.api.WALReaderSupplier;
import org.embedded.key.value.api.Key;
import org.embedded.key.value.api.KeyValue;
import org.embedded.key.value.api.Value;
import org.embedded.operation.api.Operation;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;

/**
 * Created by aakash on 9/8/17.
 */
public class AvroWalReaderSupplier extends Configured implements WALReaderSupplier {

    @Override
    public WAL.Reader<WAL.Entry> getReader(String filePath) throws IOException {
        return new AvroWalReader<>(new Path(filePath), getConf(), new WalRecordInfoToWALEntryTransformer());
    }

    public static class AvroWalReader<T extends WAL.Entry> implements WAL.Reader<T> {
        private final Path filePath;
        private final Configuration configuration;
        private final DataFileReader<WalRecordInfo> dataFileReader;
        private final Function<WalRecordInfo, T> transformer;


        public AvroWalReader(Path filePath, Configuration configuration, Function<WalRecordInfo, T> transformer) throws IOException {
            this.filePath = filePath;
            this.configuration = configuration;
            this.transformer = transformer;
            SpecificDatumReader<WalRecordInfo> specificDatumReader = new SpecificDatumReader<>(WalRecordInfo.class);
            SeekableInput seekableInput = new FsInput(this.filePath, this.configuration);
            this.dataFileReader = new DataFileReader<>(seekableInput, specificDatumReader);

        }

        @Override
        public String getFilePath() {
            return this.filePath.toString();
        }

        @Override
        public Iterator<T> iterator() {
            return new Iterator<T>() {
                WalRecordInfo walRecordInfo = new WalRecordInfo();
                @Override
                public boolean hasNext() {
                    return dataFileReader.hasNext();
                }

                @Override
                public T next() {
                    try {
                        walRecordInfo = dataFileReader.next(walRecordInfo);
                        return transformer.apply(walRecordInfo);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }

        @Override
        public void close() throws Exception {
            this.dataFileReader.close();
        }
    }

    public static class WalRecordInfoToWALEntryTransformer implements Function<WalRecordInfo, WAL.Entry> {

        @Override
        public WAL.Entry apply(WalRecordInfo walRecordInfo) {
            KeyValue keyValue = new KeyValue(new Key(walRecordInfo.getKey().array()),
                    new Value(walRecordInfo.getValue().array()), walRecordInfo.getTimestamp());
            return new WAL.Entry(walRecordInfo.getSequenceId(), keyValue,
                    Operation.valueOf(walRecordInfo.getOperation().name()));
        }
    }
}
