package dsps.ass1.localapp;

import java.io.File;
import java.util.UUID;

import javax.jms.JMSException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

public class LocalApp {

    static AWSCredentialsProvider credentialsProvider;
    static AmazonS3 s3;
    static AmazonEC2 ec2;
    static AmazonSQS sqs;
    static final String REGION = "us-west-2";
    static final String L2M_QUEUE = "LocalAppToManager";
    static String L2MUrl;
    static String M2LUrl;
    static int imagesPerWorker;
    static File input;
    private static final Object lock = new Object();
    private static boolean listening = true;
    private static QueueConnection connection;

    public static void main(String[] args) throws JMSException {

    	// Connect to AWS Services
        initialize();
        
        // Read command line arguments
        readArguments(args);
        
        // Obtain a unique id for this local app
        final String localAppId = UUID.randomUUID().toString();

        // Create S3 bucket
        final String bucketName =  localAppId + "-images";
        S3Helper.createS3Bucket(s3, bucketName);
        
        // Upload images file to bucket
        S3Helper.uploadFileToS3Bucket(s3, bucketName, input, localAppId);

        // Create SQS queues
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("ManagerTo");
        stringBuilder.append(localAppId);

        final String M2LQueue = stringBuilder.toString();
        
        L2MUrl = SQSHelper.createSQSQueue(sqs, L2M_QUEUE);
        M2LUrl = SQSHelper.createSQSQueue(sqs, M2LQueue);
        
        // Create task message
        NewTask task = new NewTask(
                bucketName, localAppId + "-" + input.getName(), imagesPerWorker, localAppId);
        
        // Send message to SQS queue
        SQSHelper.sendSQSMessage(sqs, L2MUrl, task.toString());
        
        // Start manager node, if not active already
        EC2Helper.startManagerIfNotActive(ec2);
        
        // Create an SQS connection to M2LQueue
        connection = new QueueConnection(
                REGION, credentialsProvider, M2LQueue, new LocalAppListener());
        
        // Start listening for response message from manager
        connection.start();
        
        // Await response
        synchronized(lock) {
            while (isListening()) {
                try {
                    lock.wait();
                } catch (InterruptedException ignored) {}
            }
        }
        
        // Terminate
        try {
            connection.close();
            S3Helper.deleteS3Bucket(s3, bucketName);
            sqs.deleteQueue(M2LUrl);
        } catch (AmazonClientException ex) {
            System.err.println("Caught an exception while terminating.\nClosing LocalApp.");
            throw ex;
        }
        System.out.println("All done. Output file is output.html");
    }

    private static boolean isListening() {
        return listening;
    }
    
    static void stopListening() {
        synchronized(lock) {
            listening = false;
            lock.notifyAll();
        }
    }

    private static void readArguments(String[] args) {
        try {
            if (!(input = new File(args[0])).isFile()) {
                System.err.println("Input file does not exist");
                System.exit(1);
            }
            imagesPerWorker = Integer.parseInt(args[1]);
        } catch (ArrayIndexOutOfBoundsException | NumberFormatException ex) {
            System.err.println(
                    "Usage: java -jar localapp-1.0.0-jar-with-dependencies.jar inputFileName n");
            throw ex;
        }
    }

    private static void initialize() {
        // Get credentials
        credentialsProvider = new AWSStaticCredentialsProvider(
                new ProfileCredentialsProvider().getCredentials());

        // Connect to S3 cloud
        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(REGION)
                .build();

        // Connect to SQS Service
        sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(REGION)
                .build();

        // Connect to EC2 cloud
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(REGION)
                .build();
    }
        
}
