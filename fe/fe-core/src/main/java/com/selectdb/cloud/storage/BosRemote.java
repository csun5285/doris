package com.selectdb.cloud.storage;

import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.http.HttpMethodName;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.doris.common.DdlException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;

public class BosRemote extends DefaultRemote {
    private static final Logger LOG = LogManager.getLogger(BosRemote.class);

    public BosRemote(ObjectInfo obj) {
        super(obj);
    }

    @Override
    public String getPresignedUrl(String fileName) {
        BosClientConfiguration config = new BosClientConfiguration();
        config.setCredentials(new DefaultBceCredentials(obj.getAk(), obj.getSk()));
        config.setEndpoint(obj.getEndpoint());
        BosClient client = new BosClient(config);
        URL url = client.generatePresignedUrl(obj.getBucket(), normalizePrefix(fileName), 3600, HttpMethodName.PUT);
        LOG.info("Bos getPresignedUrl: {}", url);
        return url.toString();
    }

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        throw new DdlException("Get sts token for BOS is unsupported");
    }

    @Override
    public String toString() {
        return "BosRemote{"
            + "obj=" + obj
            + '}';
    }
}
