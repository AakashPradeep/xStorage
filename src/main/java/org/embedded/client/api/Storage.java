package org.embedded.client.api;

/**
 * Created by aakash on 9/8/17.
 */
public class Storage {
    private final Namespace namespace;
    private final StorageName storageName;

    public Storage(Namespace namespace, StorageName storageName) {
        this.namespace = namespace;
        this.storageName = storageName;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public StorageName getStorageName() {
        return storageName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Storage storage = (Storage) o;

        if (namespace != null ? !namespace.equals(storage.namespace) : storage.namespace != null) return false;
        return storageName != null ? storageName.equals(storage.storageName) : storage.storageName == null;
    }

    @Override
    public int hashCode() {
        int result = namespace != null ? namespace.hashCode() : 0;
        result = 31 * result + (storageName != null ? storageName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Storage{" +
                "namespace=" + namespace +
                ", storageName=" + storageName +
                '}';
    }
}
