package org.embedded.client.api;

import com.google.common.base.Stopwatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.embedded.configuration.util.RootPathProvider;
import org.embedded.configuration.util.WorkPathProvider;
import org.embedded.dfs.key.value.storage.levels.api.CompactorExecutor;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;

/**
 * Created by aakash on 9/11/17.
 */
public class ConnectionTest {
    @Test
    public void testSimpleEnd2End() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("root.path", "/tmp/root_1/data/" + this.getClass().getSimpleName()+"/testSimpleEnd2End");
        configuration.set("work.path", "/tmp/work_1/data/" + this.getClass().getSimpleName()+"/testSimpleEnd2End");
        FileSystem fileSystem = FileSystem.get(configuration);
        Storage storage = new Storage(Namespace.from("com.aakash"), StorageName.from("test"));
        try {
            try (Connection connection = ConnectionManager.getInstance().getConnection(storage, configuration)) {
                byte[] value = "world".getBytes();
                byte[] key = "hello".getBytes();
                connection.put(key, value);
                Assert.assertThat(connection.get(key), is(value));
            }
        }finally {
            fileSystem.delete(new Path(new RootPathProvider().apply(configuration)), true);
            fileSystem.delete(new Path(new WorkPathProvider().apply(configuration)), true);

        }

    }

    @Test
    public void     testMillionRowsEnd2End() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("root.path", "/tmp/root_1/data/" + this.getClass().getSimpleName()+"/testMillionRowsEnd2End");
        configuration.set("work.path", "/tmp/work_1/data/" + this.getClass().getSimpleName()+"/testMillionRowsEnd2End");
        FileSystem fileSystem = FileSystem.get(configuration);
        Storage storage = new Storage(Namespace.from("com.aakash"), StorageName.from("test"));
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        try {

            try (Connection connection = ConnectionManager.getInstance().getConnection(storage, configuration)) {
                for (int i = 0; i < 10_000_001; i++) {
                    byte[] value = ("world" + i).getBytes();
                    byte[] key = ("hello" + i).getBytes();
                    connection.put(key, value);
                }
                stopwatch.stop();
                byte[] value = ("world0").getBytes();
                byte[] key = ("hello0").getBytes();
                System.out.println("Elapsed time: " + stopwatch.elapsedMillis() + " in seconds:" + stopwatch.elapsedTime(TimeUnit.SECONDS));
                Assert.assertThat(connection.get(key), is(value));

                CompactorExecutor.getInstance().getFixedExecutorService().awaitTermination(3, TimeUnit.HOURS);

                Assert.assertThat(connection.get(key), is(value));
                byte[] bytes = connection.get("hello100".getBytes());
                Assert.assertThat(bytes, is("world100".getBytes()));
            }
        }finally {
//            fileSystem.delete(new Path(new RootPathProvider().apply(configuration)), true);
//            fileSystem.delete(new Path(new WorkPathProvider().apply(configuration)), true);

        }

    }
}
