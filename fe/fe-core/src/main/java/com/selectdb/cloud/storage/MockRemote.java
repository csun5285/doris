package com.selectdb.cloud.storage;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.doris.common.DdlException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockRemote extends RemoteBase {
    private static final Logger LOG = LogManager.getLogger(MockRemote.class);

    private Map<String, ObjectFile> objects = new HashMap<>();

    public MockRemote(ObjectInfo obj) {
        super(obj);
    }

    public void addObject(String key) {
        objects.put(key, new ObjectFile(key, null, DigestUtils.md5Hex(key), key.length()));
    }

    public void addObjectFile(ObjectFile objectFile) {
        objects.put(objectFile.getKey(), objectFile);
    }

    @Override
    public ListObjectsResult listObjects(String continuationToken) throws DdlException {
        return listObjectsInner(normalizePrefix(), continuationToken);
    }

    @Override
    public ListObjectsResult listObjects(String subPrefix, String continuationToken) throws DdlException {
        return listObjectsInner(normalizePrefix(subPrefix), continuationToken);
    }

    @Override
    public ListObjectsResult headObject(String subKey) throws DdlException {
        List<ObjectFile> objectInfoList = new ArrayList<>();
        String key = normalizePrefix(subKey);
        if (objects.containsKey(key)) {
            ObjectFile objectFile = objects.get(key);
            objectInfoList.add(new ObjectFile(key, getRelativePath(key), objectFile.getEtag(), objectFile.getSize()));
        }
        return new ListObjectsResult(objectInfoList, false, null);
    }

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        throw new DdlException("Get sts token for Mock is unsupported");
    }

    private ListObjectsResult listObjectsInner(String prefix, String continuationToken) throws DdlException {
        List<ObjectFile> objectFiles = new ArrayList<>();
        for (ObjectFile objectFile : objects.values()) {
            if (objectFile.getKey().startsWith(prefix)) {
                objectFiles.add(
                        new ObjectFile(objectFile.getKey(), getRelativePath(objectFile.getKey()), objectFile.getEtag(),
                                objectFile.getSize()));
            }
        }
        return new ListObjectsResult(objectFiles, false, null);
    }
}
