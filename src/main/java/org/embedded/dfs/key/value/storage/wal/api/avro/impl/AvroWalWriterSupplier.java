package org.embedded.dfs.key.value.storage.wal.api.avro.impl;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.embedded.dfs.key.value.storage.wal.api.WAL;
import org.embedded.dfs.key.value.storage.wal.api.WalWriterSupplier;
import org.embedded.key.value.api.KeyValue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Created by aakash on 9/8/17.
 */
public class AvroWalWriterSupplier extends Configured implements WalWriterSupplier {
    public static final String WAL_AVRO_COMPRESSION_CODEC_NAME = "wal.avro.compression.codec.name";

    @Override
    public <T extends WAL.Entry> WAL.Writer<T> getWriter(String filePath) throws IOException {
        Configuration configuration = getConf();
        String codecName = configuration.get(WAL_AVRO_COMPRESSION_CODEC_NAME, "snappy");
        FileSystem fileSystem = FileSystem.get(configuration);
        return new AvroFileWriter<>(fileSystem.create(new Path(filePath)), CodecFactory.fromString(codecName));
    }


    public static class AvroFileWriter<T extends WAL.Entry> implements WAL.Writer<T> {
        private final DataFileWriter<WalRecordInfo> dataFileWriter;

        public AvroFileWriter(OutputStream outputStream, CodecFactory codecFactory) throws IOException {
            SpecificDatumWriter<WalRecordInfo> specificDatumWriter = new SpecificDatumWriter<>(WalRecordInfo.class);
            this.dataFileWriter = new DataFileWriter<>(specificDatumWriter);
            this.dataFileWriter.setCodec(codecFactory);
            this.dataFileWriter.create(WalRecordInfo.getClassSchema(), outputStream);
        }


        @Override
        public boolean write(T entry) throws IOException {
            KeyValue keyValue = entry.getKeyValue();
            WalRecordInfo walRecordInfo = WalRecordInfo.newBuilder().setSequenceId(entry.getSequenceId())
                    .setKey(ByteBuffer.wrap(keyValue.key().getValue()))
                    .setValue(ByteBuffer.wrap(keyValue.value().getValue()))
                    .setTimestamp(keyValue.timestamp())
                    .setOperation(operation.valueOf(entry.getOperation().name())).build();
            this.dataFileWriter.append(walRecordInfo);
            return true;
        }


        @Override
        public void close() throws Exception {
            this.dataFileWriter.close();
        }
    }
}
