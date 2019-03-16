

# xStorage : An Introduction
An **embedded key value storage system with reliable distributed persistence**. Current implementation uses HDFS as distributed persistence but idea is to use any distributed filesystem or object storage like S3 or GCS and it should be pluggable for each storage. Since its embedded, there can be only one client application which can read and write from the storage system with strong consistency. Since persistence is distributed, client can move to different hosts as long as persisted data is accessible.

The design of xStorgae system is highly influenced from [Level DB](https://github.com/google/leveldb) and [Rocks DB](https://rocksdb.org/). It uses [LSM](https://en.wikipedia.org/wiki/Log-structured_merge-tree) to provide high write performance and reliability, and it will be regularaly flushed to disk (persistence layer) based on size and time. 

Data in persistence layer is organized in different levels, generally max level is 7, for following purposes:

* minimize the number of files which also reduces the amount of lookup.
* data get promoted from one level to the next i.e. from Level 0 to Level1 using Compaction concept, which also helps in reducing the data footprint. 
* Data in persistence layer is immutable and if a key is intended to be deleted then its another entry in storage which is handled during Compaction based on other action and its timeline. 
* Level also indicates timeline for a key, key in lower level is recent than higher level. This feature is used for key lookup. 

# Features
* Keys and values are arbitrary byte arrays.
* Data is stored sorted by key.
* The basic operations are Put(key,value), Get(key), Delete(key)
* Data is automatically compressed
* Highly reliable as persistence layer is distributed in nature
* As data is not stored on local filesystem, Client application is not limited to a single host rather it can be instantiated on any host as long data is accessible. 
* It supports namespace, so same persistence layer can be used with different clients.

# Desired feature
* Pluggable persistence layer like S3, [GCS](https://cloud.google.com/storage/getting-started/) for each storage/namespace
* SST or HFile like implementation for storage instead of  HDFS Bloom file and this should be pluggable for each storage/namespace
* Support range scan
* Global Index to make search fast so that search across multiple level can be avoided.
* Support Snapshot

# Where I can be used 
* As a DB for [YARN Resource Manager](https://hortonworks.com/blog/apache-hadoop-yarn-resourcemanager/) 
* As a DB for [Job History Server](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/HistoryServerRest.html)    
* It can be used with any application (app server or web server) to persist its state for example a typical use would be to use with an application running as docker container to persist any local scope data. As docker container is ephemeral in nature, it can go down and restart on any node in cloud, storage system like **xStorage** can help it maintain highly consistent state/business data and make the system highly reliable and available.


# How to use: an example
```Java
    final Storage storage = new Storage(Namespace.from("com.aakash"), StorageName.from("test"));
    final Configuration configuration = new Configuration();
    try (Connection connection = ConnectionManager.getInstance().getConnection(storage, configuration)) {
        byte[] value = "world".getBytes();
        byte[] key = "hello".getBytes();
        connection.put(key, value);
        Assert.assertThat(connection.get(key), is(value));
    }
```
# License 
This project is licensed under [The GPL License]https://en.wikipedia.org/wiki/GNU_General_Public_License

# Authors
Aakash Pradeep (email2aakash@gmail.com)
