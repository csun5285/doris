package com.selectdb.cloud.objectsigner;

import com.selectdb.cloud.proto.SelectdbCloud;

import lombok.Getter;

public class RemoteBase {
    @Getter
    public static class ObjectInfo {
        private final SelectdbCloud.ObjectStoreInfoPB.Provider provider;
        private final String ak;
        private final String sk;
        private final String bucket;
        private final String endpoint;
        private final String region;
        private final String prefix;

        public ObjectInfo(SelectdbCloud.ObjectStoreInfoPB.Provider provider,
                          String ak, String sk, String bucket, String endpoint, String region, String prefix) {
            this.provider = provider;
            this.ak = ak;
            this.sk = sk;
            this.bucket = bucket;
            this.endpoint = endpoint;
            this.region = region;
            this.prefix = prefix;
        }

        @Override
        public String toString() {
            return "Obj{"
                + "provider=" + provider
                + ", ak='" + ak + '\''
                + ", sk='" + sk + '\''
                + ", bucket='" + bucket + '\''
                + ", endpoint='" + endpoint + '\''
                + ", region='" + region + '\''
                + ", prefix='" + prefix + '\''
                + '}';
        }
    }

    public ObjectInfo obj;

    public RemoteBase(ObjectInfo obj) {
        this.obj = obj;
    }

    public String getPresignedUrl(String fileName) {
        return "not impl";
    }

    public static RemoteBase newInstance(ObjectInfo obj) throws Exception {
        switch (obj.provider) {
            case OSS:
                return new OssRemote(obj);
            case S3:
                return new S3Remote(obj);
            case COS:
                return new CosRemote(obj);
            case OBS:
                return new ObsRemote(obj);
            case BOS:
                return new BosRemote(obj);
            default:
                throw new Exception("current not support obj : " + obj.toString());
        }
    }
}
