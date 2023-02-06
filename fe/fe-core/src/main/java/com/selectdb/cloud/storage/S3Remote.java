package com.selectdb.cloud.storage;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.doris.common.DdlException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.net.URL;
import java.time.Duration;

public class S3Remote extends DefaultRemote {
    private static final Logger LOG = LogManager.getLogger(S3Remote.class);

    public S3Remote(ObjectInfo obj) {
        super(obj);
    }

    @Override
    public String getPresignedUrl(String fileName) {
        try {
            PutObjectRequest objectRequest = PutObjectRequest.builder()
                    .bucket(obj.getBucket())
                    .key(normalizePrefix(fileName))
                    .build();

            PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofMinutes(60))
                    .putObjectRequest(objectRequest)
                    .build();

            AwsBasicCredentials credentialsProvider = AwsBasicCredentials.create(obj.getAk(), obj.getSk());
            Region region = Region.of(obj.getRegion());
            S3Presigner presigner = S3Presigner.builder()
                    .region(region)
                    .credentialsProvider(StaticCredentialsProvider.create(credentialsProvider))
                    .build();

            PresignedPutObjectRequest presignedRequest = presigner.presignPutObject(presignRequest);
            String presignedURL = presignedRequest.url().toString();
            LOG.info("Presigned URL to upload a file to: {}", presignedURL);

            // Upload content to the Amazon S3 bucket by using this URL.
            URL url = presignedRequest.url();
            return url.toString();

        } catch (S3Exception e) {
            e.getStackTrace();
        }
        return "";
    }

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        try {
            AwsBasicCredentials basicCredentials = AwsBasicCredentials.create(obj.getAk(), obj.getSk());
            StaticCredentialsProvider scp = StaticCredentialsProvider.create(basicCredentials);
            StsClient stsClient = StsClient.builder().credentialsProvider(scp)
                    .region(Region.of(obj.getRegion())).build();
            AssumeRoleRequest request = AssumeRoleRequest.builder().roleArn(obj.getArn())
                    .durationSeconds(getDurationSeconds())
                    .roleSessionName(getNewRoleSessionName()).externalId(obj.getExternalId()).build();
            AssumeRoleResponse assumeRoleResponse = stsClient.assumeRole(request);
            Credentials credentials = assumeRoleResponse.credentials();
            return Triple.of(credentials.accessKeyId(), credentials.secretAccessKey(), credentials.sessionToken());
        } catch (Throwable e) {
            LOG.warn("Failed get s3 sts token", e);
            throw new DdlException(e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "S3Remote{"
            + "obj=" + obj
            + '}';
    }
}
