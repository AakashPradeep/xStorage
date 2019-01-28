package org.embedded.dfs.key.value.storage;

import org.embedded.client.api.Storage;

public class FilePathProvider {
    protected final Storage storage;
    protected final String rootPath;
    protected final StorageType storageType;

    public FilePathProvider(Storage storage, String rootPath, StorageType storageType) {
        this.storage = storage;
        this.rootPath = rootPath;
        this.storageType = storageType;
    }

    public String getStorageDir() {
        StringBuilder filePathBuilder = new StringBuilder(this.rootPath);
        filePathBuilder.append("/").append(getPathPart("namespace", this.storage.getNamespace().toString())).
                append("/").append(getPathPart("storageName", this.storage.getStorageName().toString()))
                .append("/").append(getPathPart("storageType", storageType.toString()));
        return filePathBuilder.toString();
    }

    public String getFilePath(String filename) {
        StringBuilder filePathBuilder = new StringBuilder();
        filePathBuilder.append(getStorageDir()).append("/").append(filename);
        return filePathBuilder.toString();
    }

    String getPathPart(String keyname, String value) {
        return keyname + "=" + value;
    }
}