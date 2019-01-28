package org.embedded.dfs.key.value.storage.levels.api.impl;

/**
 * Created by aakash on 9/12/17.
 */

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.embedded.configuration.util.LevelFileCompressionCodecProvider;
import org.embedded.dfs.key.value.storage.levels.api.LevelFileValueWritable;

import java.io.IOException;

/**
 * Class to merge multiple BloomMapFiles of same Key and Value types to one BloomMapFile
 */
public  class BloomMapFilesMerger {
    private Configuration configuration;
    private WritableComparator comparator = null;
    private MapFile.Reader[] inReaders;
    private MapFile.Writer outWriter;
    private Class<Writable> valueClass = null;
    private Class<WritableComparable> keyClass = null;

    public BloomMapFilesMerger(Configuration configuration) throws IOException {
        this.configuration = configuration;
    }

    /**
     * Merge multiple MapFiles to one Mapfile
     *
     * @param inMapFiles
     * @param outMapFile
     * @throws IOException
     */
    public void merge(Path[] inMapFiles, boolean deleteInputs,
                      Path outMapFile) throws IOException {
        try {
            open(inMapFiles, outMapFile);
            mergePass();
        } finally {
            close();
        }
        if (deleteInputs) {
            for (int i = 0; i < inMapFiles.length; i++) {
                Path path = inMapFiles[i];
                FileSystem.get(configuration).delete(path,true);
            }
        }
    }

    /*
     * Open all input files for reading and verify the key and value types. And
     * open Output file for writing
     */
    @SuppressWarnings("unchecked")
    private void open(Path[] inBloomMapFiles, Path outBloomMapFile) throws IOException {
        inReaders = new BloomMapFile.Reader[inBloomMapFiles.length];
        for (int i = 0; i < inBloomMapFiles.length; i++) {
            BloomMapFile.Reader reader = new BloomMapFile.Reader(inBloomMapFiles[i], configuration);
            if (keyClass == null || valueClass == null) {
                keyClass = (Class<WritableComparable>) reader.getKeyClass();
                valueClass = (Class<Writable>) reader.getValueClass();
            } else if (keyClass != reader.getKeyClass()
                    || valueClass != reader.getValueClass()) {
                throw new HadoopIllegalArgumentException(
                        "Input files cannot be merged as they"
                                + " have different Key and Value classes");
            }
            inReaders[i] = reader;
        }

        if (comparator == null) {
            comparator = new ByteWritable.Comparator();
        } else if (comparator.getKeyClass() != keyClass) {
            throw new HadoopIllegalArgumentException(
                    "Input files cannot be merged as they"
                            + " have different Key class compared to"
                            + " specified comparator");
        }

        outWriter = getBloomFilterWriter(outBloomMapFile.toString());
    }

    /**
     * Merge all input files to output map file.<br>
     * 1. Read first key/value from all input files to keys/values array. <br>
     * 2. Select the least key and corresponding value. <br>
     * 3. Write the selected key and value to output file. <br>
     * 4. Replace the already written key/value in keys/values arrays with the
     * next key/value from the selected input <br>
     * 5. Repeat step 2-4 till all keys are read. <br>
     */
    private void mergePass() throws IOException {
        // re-usable array
        BytesWritable[] keys = new BytesWritable[inReaders.length];
        LevelFileValueWritable[] values = new LevelFileValueWritable[inReaders.length];
        // Read first key/value from all inputs
        for (int i = 0; i < inReaders.length; i++) {
            keys[i] = new BytesWritable();
            values[i] = new LevelFileValueWritable();
            if (!inReaders[i].next(keys[i], values[i])) {
                // Handle empty files
                keys[i] = null;
                values[i] = null;
            }
        }

        do {
            int currentEntry = -1;
            BytesWritable currentKey = null;
            LevelFileValueWritable currentValue = null;
            for (int i = 0; i < keys.length; i++) {
                if (keys[i] == null) {
                    // Skip Readers reached EOF
                    continue;
                }
                if (currentKey == null || comparator.compare(currentKey, keys[i]) > 0) {
                    currentEntry = i;
                    currentKey = keys[i];
                    currentValue = values[i];
                }
                else if(currentKey !=null && comparator.compare(currentKey,keys[i]) ==0){
                    if(currentValue.getTimestamp() < values[i].getTimestamp()){
                        currentKey = keys[i];
                        currentValue = values[i];
                        currentEntry =i;
                    }
                }
            }
            if (currentKey == null) {
                // Merge Complete
                break;
            }
            // Write the selected key/value to merge stream
            outWriter.append(currentKey, currentValue);
            // Replace the already written key/value in keys/values arrays with the
            // next key/value from the selected input
            if (!inReaders[currentEntry].next(keys[currentEntry],
                    values[currentEntry])) {
                // EOF for this file
                keys[currentEntry] = null;
                values[currentEntry] = null;
            }
        } while (true);
    }

    private void close() throws IOException {
        for (int i = 0; i < inReaders.length; i++) {
            IOUtils.closeStream(inReaders[i]);
            inReaders[i] = null;
        }
        if (outWriter != null) {
            outWriter.close();
            outWriter = null;
        }
    }

    protected BloomMapFile.Writer getBloomFilterWriter(String workingFilePath) throws IOException {
        LevelFileCompressionCodecProvider levelFileCompressionCodecProvider = new LevelFileCompressionCodecProvider();

        if (levelFileCompressionCodecProvider.isCompressionEnabled(configuration)) {
            CompressionCodec compressionCodec = levelFileCompressionCodecProvider.apply(this.configuration);
            return new BloomMapFile.Writer(this.configuration, new Path(workingFilePath),
                    BloomMapFile.Writer.keyClass(BytesWritable.class),
                    BloomMapFile.Writer.valueClass(LevelFileValueWritable.class),
                    BloomMapFile.Writer.compression(SequenceFile.CompressionType.BLOCK, compressionCodec));
        }

        return new BloomMapFile.Writer(this.configuration, new Path(workingFilePath),
                BloomMapFile.Writer.keyClass(BytesWritable.class),
                BloomMapFile.Writer.valueClass(LevelFileValueWritable.class));
    }
}
