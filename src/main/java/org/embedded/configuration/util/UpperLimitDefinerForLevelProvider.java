package org.embedded.configuration.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.embedded.dfs.key.value.storage.levels.api.DefaultUpperLimitDefinerForLevel;
import org.embedded.dfs.key.value.storage.levels.api.UpperLimitDefinerForLevel;

import java.util.function.Function;

/**
 * Created by aakash on 9/10/17.
 */
public class UpperLimitDefinerForLevelProvider implements Function<Configuration, UpperLimitDefinerForLevel> {
    @Override
    public UpperLimitDefinerForLevel apply(Configuration configuration) {
        Class<?> upperLimitDefinerClass = configuration.getClass("level.define.uper.limit.impl.class",
                DefaultUpperLimitDefinerForLevel.class);
        return (UpperLimitDefinerForLevel) ReflectionUtils.newInstance(upperLimitDefinerClass,
                configuration);
    }
}
