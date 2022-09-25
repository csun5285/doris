// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.backup;

import org.apache.doris.persist.EditLog;

import mockit.Mocked;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

@Ignore
public class S3StorageTest {
    private static String basePath;
    private final String bucket = "s3://doris-test/";
    private Map<String, String> properties;
    private S3Storage storage;
    private String testFile;
    private String content;

    @Mocked
    private EditLog editLog;

    @BeforeClass
    public static void init() {
        basePath = "s3/" + UUID.randomUUID().toString();
    }

    @Before
    public void setUp() throws Exception {
        properties = new HashMap<>();
        properties.put("AWS_ACCESS_KEY", System.getenv().getOrDefault("AWS_AK", ""));
        properties.put("AWS_SECRET_KEY", System.getenv().getOrDefault("AWS_SK", ""));
        properties.put("AWS_ENDPOINT", "http://s3.bj.bcebos.com");
        properties.put(S3Storage.USE_PATH_STYLE, "false");

        properties.put("AWS_REGION", "bj");
        storage = new S3Storage(properties);
        testFile = bucket + basePath + "/Ode_to_the_West_Wind";

        content =
                "O wild West Wind, thou breath of Autumn's being\n"
                        + "Thou, from whose unseen presence the leaves dead\n"
                        + "Are driven, like ghosts from an enchanter fleeing,\n"
                        + "Yellow, and black, and pale, and hectic red,\n"
                        + "Pestilence-stricken multitudes:O thou\n"
                        + "Who chariotest to their dark wintry bed\n"
                        + "The winged seeds, where they lie cold and low,\n"
                        + "Each like a corpse within its grave, until\n"
                        + "Thine azure sister of the Spring shall blow\n"
                        + "Her clarion o'er the dreaming earth, and fill\n"
                        + "(Driving sweet buds like flocks to feed in air)\n"
                        + "With living hues and odors plain and hill:\n"
                        + "Wild Spirit, which art moving everywhere;\n"
                        + "Destroyer and preserver; hear, oh, hear!";
        Assert.assertEquals(Status.OK, storage.directUpload(content, testFile));
    }

    @Test
    public void downloadWithFileSize() throws IOException {
        File localFile = File.createTempFile("s3unittest", ".dat");
        localFile.deleteOnExit();
        Status status = storage.downloadWithFileSize(testFile, localFile.getAbsolutePath(), content.getBytes().length);
        Assert.assertEquals(Status.OK, status);
        Assert.assertEquals(DigestUtils.md5Hex(content.getBytes()), DigestUtils.md5Hex(new FileInputStream(localFile)));
        status = storage.downloadWithFileSize(bucket + basePath + "/Ode_to_the_West_Wind", localFile.getAbsolutePath(), content.getBytes().length + 1);
        Assert.assertNotEquals(Status.OK, status);
    }

    @Test
    public void upload() throws IOException {
        File localFile = File.createTempFile("s3unittest", ".dat");
        localFile.deleteOnExit();
        OutputStream os = new FileOutputStream(localFile);
        byte[] buf = new byte[1024 * 1024];
        Random r = new Random();
        r.nextBytes(buf);
        os.write(buf);
        os.close();
        String remote = bucket + basePath + "/" + localFile.getName();
        Status status = storage.upload(localFile.getAbsolutePath(), remote);
        Assert.assertEquals(Status.OK, status);
        File localFile2 = File.createTempFile("s3unittest", ".dat");
        localFile2.deleteOnExit();
        status = storage.downloadWithFileSize(remote, localFile2.getAbsolutePath(), 1024 * 1024);
        Assert.assertEquals(Status.OK, status);
        Assert.assertEquals(DigestUtils.md5Hex(new FileInputStream(localFile)),
                DigestUtils.md5Hex(new FileInputStream(localFile2)));
    }

    @Test
    public void copy() {
        Assert.assertEquals(Status.OK, storage.copy(testFile, testFile + ".bak"));
        Assert.assertEquals(Status.OK, storage.checkPathExist(testFile + ".bak"));
        Assert.assertNotEquals(Status.OK, storage.copy(testFile + ".bakxxx", testFile + ".bak"));
    }

    @Test
    public void rename() {
        Assert.assertEquals(Status.OK, storage.directUpload(content, testFile + ".bak"));
        storage.rename(testFile + ".bak", testFile + ".bak1");
        Assert.assertEquals(Status.ErrCode.NOT_FOUND, storage.checkPathExist(testFile + ".bak").getErrCode());
        Assert.assertEquals(Status.OK, storage.checkPathExist(testFile + ".bak1"));
    }

    @Test
    public void delete() {
        String deleteFile = testFile + ".to_be_delete";
        Assert.assertEquals(Status.OK, storage.directUpload(content, deleteFile));
        Assert.assertEquals(Status.OK, storage.delete(deleteFile));
        Assert.assertEquals(Status.ErrCode.NOT_FOUND, storage.checkPathExist(deleteFile).getErrCode());
        Assert.assertEquals(Status.OK, storage.delete(deleteFile + "xxxx"));
    }

    @Test
    public void list() {
        List<RemoteFile> result = new ArrayList<>();
        String listPath = bucket + basePath + "_list" + "/Ode_to_the_West_Wind";
        Assert.assertEquals(Status.OK, storage.directUpload(content, listPath + ".1"));
        Assert.assertEquals(Status.OK, storage.directUpload(content, listPath + ".2"));
        Assert.assertEquals(Status.OK, storage.directUpload(content, listPath + ".3"));
        Assert.assertEquals(Status.OK, storage.list(bucket + basePath + "_list/*", result));
        Assert.assertEquals(3, result.size());
    }

    @Test
    public void makeDir() {
        String path = bucket + basePath + "/test_path";
        Assert.assertEquals(Status.OK, storage.makeDir(path));
        Assert.assertNotEquals(Status.OK, storage.checkPathExist(path));
        String path1 = bucket + basePath + "/test_path1/";
        Assert.assertEquals(Status.OK, storage.makeDir(path1));
        Assert.assertEquals(Status.OK, storage.checkPathExist(path1));
    }

    @Test
    public void checkPathExist() {
        Status status = storage.checkPathExist(testFile);
        Assert.assertEquals(Status.OK, status);
        status = storage.checkPathExist(testFile + ".NOT_EXIST");
        Assert.assertEquals(Status.ErrCode.NOT_FOUND, status.getErrCode());
    }

    @Ignore
    @Test
    public void listObjectsWithSize() throws IOException {
        // new S3Storage to use a new S3Client
        S3Storage s3Storage = new S3Storage(properties);
        String bucket = "s3://justtmp-bj-1308700295/meiyi_cloud_test/";
        String path = bucket; // + "1_my.csv";


        // size_limit = 0 and pattern = null;
        do {
            List<RemoteFile> result = new ArrayList<>();
            Assert.assertEquals(Status.OK, s3Storage.list(0, path, null, 0, result));
            for (RemoteFile remoteFile : result) {
                System.out.println("remote file = " + remoteFile.getName());
            }
        } while (false);
        // size_limit > 0 and pattern = null;
        do {
            List<RemoteFile> result = new ArrayList<>();
            int i = 0;
            int size;
            do {
                size = result.size();
                i++;
                Assert.assertEquals(Status.OK, s3Storage.list(1, path, null, 30, result));
                for (RemoteFile remoteFile : result) {
                    System.out.println("remote file " + i + " =" + remoteFile.getName());
                }
            } while (size != result.size());
        } while (false);
        // size_limit = 0 and pattern != null
        do {
            List<RemoteFile> result = new ArrayList<>();
            String pattern = ".*my.*";
            Assert.assertEquals(Status.OK, s3Storage.list(2, path, pattern, 0, result));
            for (RemoteFile remoteFile : result) {
                System.out.println("remote file with pattern = " + remoteFile.getName());
            }
        } while (false);
        // size_limit > 0 and pattern != null
        do {
            List<RemoteFile> result = new ArrayList<>();
            String pattern = ".*my.*";
            int i = 0;
            int size;
            do {
                size = result.size();
                i++;
                Assert.assertEquals(Status.OK, s3Storage.list(4, path, pattern, 30, result));
                for (RemoteFile remoteFile : result) {
                    System.out.println("remote file with pattern " + i + " =" + remoteFile.getName());
                }
            } while (size != result.size());
        } while (false);
    }
}
