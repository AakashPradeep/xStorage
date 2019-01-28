package org.embedded.client.api;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.ConcurrentMap;

/**
 * Created by aakash on 9/10/17.
 */
public class ConnectionManager {
    private static final ConcurrentMap<Storage, Connection> storageConnectionConcurrentMap =
            Maps.newConcurrentMap();

    private static final ConnectionManager INSTANCE = new ConnectionManager();

    public static ConnectionManager getInstance() {
        return INSTANCE;
    }

    public Connection getConnection(Storage storage, final Configuration configuration) {
        return storageConnectionConcurrentMap.computeIfAbsent(storage, s ->
                new Connection(s, configuration));
    }
}
