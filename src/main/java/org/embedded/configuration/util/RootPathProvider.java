package org.embedded.configuration.util;

import org.apache.hadoop.conf.Configuration;

import java.util.function.Function;

public class RootPathProvider implements Function<Configuration, String> {

    @Override
    public String apply(Configuration configuration) {
        return configuration.get("root.path", ".");
    }
}