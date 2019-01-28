package org.embedded.configuration.util;

import org.apache.hadoop.conf.Configuration;

import java.util.function.Function;

/**
 * Created by aakash on 9/10/17.
 */
public class WorkPathProvider implements Function<Configuration, String> {
    @Override
    public String apply(Configuration configuration) {
        return configuration.get("work.path", "./work");
    }
}
