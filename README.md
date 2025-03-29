# xStorage

**xStorage** is a high-performance, embedded key-value storage system with **pluggable distributed persistence**. It is designed to work seamlessly with distributed filesystems such as **HDFS**, **Amazon S3**, and **Google Cloud Storage (GCS)**, allowing applications to decouple storage from compute while maintaining strong consistency and fault tolerance.

Unlike traditional embedded storage engines, xStorage allows a single client to write and read data across hosts, as long as the underlying distributed storage is accessible — making it ideal for ephemeral environments like containers or cloud-native applications.

---

## 🚀 Key Features

- 🔐 **Embedded & Consistent**: One client instance at a time ensures strong consistency.
- 🌐 **Distributed Persistence**: Supports HDFS and is pluggable for other storages like S3 or GCS.
- 🗃️ **Namespace Isolation**: Multiple logical clients can share the same storage backend via namespaces.
- 📦 **LSM-based Design**: Inspired by [LevelDB](https://github.com/google/leveldb) and [RocksDB](https://rocksdb.org/), optimized for write-heavy workloads.
- 🔁 **Data Compaction**: Multi-level compaction reduces footprint and improves lookup performance.
- 📄 **Sorted Storage**: Keys are stored in sorted order with automatic compression.
- 💾 **Host-Agnostic**: Data is not tied to local filesystem, enabling clients to move freely across hosts.
- 🔄 **Basic Operations**: `Put(key, value)`, `Get(key)`, `Delete(key)`

---

## 🧠 How It Works

- Uses **Log-Structured Merge Trees (LSM)** to optimize for write-heavy operations.
- Data is flushed from memory to the persistence layer periodically based on size and time thresholds.
- Organizes data in **levels (0–7)** with promotion and compaction strategies to reduce lookup time and stale data.
- Deletions are handled via *tombstones*, and physical removal happens during compaction.
- Lower levels contain more recent data, which helps with efficient lookups.

---

## 🌟 Desired Enhancements

- ✅ Pluggable storage backend: S3, GCS, Azure Blob
- ✅ SST or HFile-like format support
- ✅ Range scan capability
- ✅ Global indexing for faster multi-level search
- ✅ Snapshot support
- ✅ Transactions for compaction

---

## 📦 Real-World Use Cases

- 🔹 As a metadata DB for [YARN ResourceManager](https://hortonworks.com/blog/apache-hadoop-yarn-resourcemanager/)
- 🔹 State store for [Job History Server](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/HistoryServerRest.html)
- 🔹 Reliable storage for ephemeral compute environments like Docker containers
- 🔹 Durable local state management for app or web servers running in distributed cloud environments

---

## 💡 Example Usage (Java)

```java
final Storage storage = new Storage(Namespace.from("com.aakash"), StorageName.from("test"));
final Configuration configuration = new Configuration();

try (Connection connection = ConnectionManager.getInstance().getConnection(storage, configuration)) {
    byte[] value = "world".getBytes();
    byte[] key = "hello".getBytes();
    connection.put(key, value);
    Assert.assertThat(connection.get(key), is(value));
}
```


## License

This project is licensed under the GNU General Public License (GPL).

## Author

Aakash Pradeep
📧 email2aakash@gmail.com

## Topics

java, key-value store, distributed storage, embedded database, LSM, docker, ephemeral compute, persistent storage, HDFS, S3, GCS

## Support & Contribution

If you find this project interesting, please ⭐️ star the repo to show your support!

Contributions and feedback are welcome — open an issue or pull request to get involved.
