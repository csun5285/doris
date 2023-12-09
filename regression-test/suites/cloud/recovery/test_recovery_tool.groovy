import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Object;
import org.codehaus.groovy.runtime.IOGroovyMethods

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

suite("test_recovery_tool") {
    def ak = getS3AK()
    def sk = getS3SK()
    def endpoint = getS3Endpoint()
    def bucketName = getS3BucketName()
    def region = getS3Region()
    def provider = getProvider().toUpperCase()

    def toolPath = "${context.file.parent}/recovery_tool"
    AwsCredentials credentials;
    credentials = AwsBasicCredentials.create(ak, sk);
    StaticCredentialsProvider scp = StaticCredentialsProvider.create(credentials);
    URI endpointUri = URI.create("http://" + "${endpoint}");
    S3Client s3Client = S3Client.builder().endpointOverride(endpointUri).credentialsProvider(scp)
            .region(Region.of(region)).build();

    def now = System.currentTimeMillis()
    String checkKey = "test_recovery/test_${now}.txt";
    String content = "check";

    // put checkKey
    PutObjectRequest request = PutObjectRequest.builder()
            .bucket(bucketName)
            .key(checkKey)
            .build();
    s3Client.putObject(request, RequestBody.fromString(content));

    // delect checkKey
    DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
            .bucket(bucketName)
            .key(checkKey)
            .build();
    s3Client.deleteObject(deleteRequest);

    // head checkKey
    HeadObjectRequest headRequest = HeadObjectRequest.builder()
            .bucket(bucketName)
            .key(checkKey)
            .build();
    boolean isExist = true
    try {
        s3Client.headObject(headRequest);
    } catch (NoSuchKeyException e) {
        isExist = false
    }
    assertTrue(isExist == false)

    // recovery checkKey
    StringBuilder strBuilder = new StringBuilder()
    strBuilder.append(""" ${toolPath} --mode=s3 --ak=${ak} --sk=${sk} --endpoint=${endpoint} --region=${region} """)
    strBuilder.append(""" --bucket=${bucketName} --provider=${provider} --object_key=${checkKey} --list_versions_nums=2 """)
    String command = strBuilder.toString()
    logger.info(" command=" + command)
    def process = command.toString().execute()
    def code = process.waitFor()
    def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    def out = process.getText()
    logger.info("code=" + code + ", out=" + out + ", err=" + err)
 
    // head again
    try {
        s3Client.headObject(headRequest);
        isExist = true
    } catch (NoSuchKeyException e) {
        isExist = false
    }
    assertTrue(isExist)

    GetObjectRequest getObjectRequest = GetObjectRequest.builder()
            .bucket(bucketName)
            .key(checkKey)
            .build();
    ResponseInputStream<GetObjectResponse> getObjectResponse = s3Client.getObject(getObjectRequest);
    String objectAsString;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(getObjectResponse))) {
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line);
        }
        objectAsString = sb.toString();
    }
    assertTrue(objectAsString.equals(content))

}