package org.embedded.configuration.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.function.Function;

/**
 * Created by aakash on 9/10/17.
 */
public class LevelFileCompressionCodecProvider implements Function<Configuration, CompressionCodec> {
    @Override
    public CompressionCodec apply(Configuration configuration) {
        Class<?> aClass = configuration.getClass("level.compactor.compression.codec.class", SnappyCodec.class);
        return (CompressionCodec) ReflectionUtils.newInstance(aClass, configuration);
    }


    public boolean isCompressionEnabled(Configuration configuration){
        return configuration.getBoolean("level.compactor.compression.enabled", false);
    }

}
