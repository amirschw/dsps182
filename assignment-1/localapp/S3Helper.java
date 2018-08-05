package dsps.ass1.localapp;

import java.io.File;
import java.util.Iterator;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3Helper {
    
    public static S3Object downloadObject(AmazonS3 s3, String bucketName, String key) {
        S3Object res = null;
        try {
            res = s3.getObject(bucketName, key);
        } catch (AmazonClientException ex) {
            System.err.println("Caught an exception while downloading S3 object");
            throw ex;
        }
        System.out.println("Downloaded file: " + key + " from S3 bucket: " + bucketName);
        return res;
    }

    public static void createS3Bucket(AmazonS3 s3, String bucketName) {
        try {
            if (!(s3.doesBucketExistV2(bucketName))) {
                s3.createBucket(new CreateBucketRequest(bucketName));
                System.out.println("S3 Bucket created: " + bucketName);
            } else {
                System.out.println("S3 Bucket already exists: " + bucketName);
            }
        } catch (AmazonClientException ex) {
            System.err.println("Caught an exception while creating S3 bucket");
            throw ex;
        }
    }
    
    public static void deleteS3Bucket(AmazonS3 s3, String bucketName) {
        try {
            ObjectListing objectListing = s3.listObjects(bucketName);
            while (true) {
                // Remove all objects from bucket 
                for (Iterator<?> iterator =
                        objectListing.getObjectSummaries().iterator();
                        iterator.hasNext();) {
                    S3ObjectSummary summary = (S3ObjectSummary)iterator.next();
                    s3.deleteObject(bucketName, summary.getKey());
                }
                
                if (objectListing.isTruncated()) {
                    objectListing = s3.listNextBatchOfObjects(objectListing);
                } else {
                    break;
                }
            }
            s3.deleteBucket(bucketName);
        } catch (AmazonClientException ex) {
            System.err.println("Caught an exception while deleting S3 bucket: " + bucketName);
            throw ex;
        }
        System.out.println("S3 bucket deleted: " + bucketName);
    }
    
    public static void uploadFileToS3Bucket(
            AmazonS3 s3, String bucketName, String key, File file) {
        try {
            s3.putObject(new PutObjectRequest(bucketName, key, file));
        } catch (AmazonClientException ex) {
            System.err.println("Caught an exception while uploading file: " +
                               key + " to S3 bucket: " + bucketName);
            throw ex;
        }
        System.out.println("Uploaded file: " + key + " to S3 bucket: " + bucketName);
    }   
    
    public static void uploadFileToS3Bucket(AmazonS3 s3, String bucketName, File file) {
        uploadFileToS3Bucket(s3, bucketName, file.getName(), file);
    }   
    
    public static void uploadFileToS3Bucket(
            AmazonS3 s3, String bucketName, File file, String localAppId) {
        uploadFileToS3Bucket(s3, bucketName, localAppId + "-" + file.getName(), file);
    }   
    
}
