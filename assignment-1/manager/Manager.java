package dsps.ass1.manager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

public class Manager {
    
    static InstanceProfileCredentialsProvider credentialsProvider;
    static AmazonS3 s3;
    static AmazonEC2 ec2;
    static AmazonSQS M2Lsqs;
    static AmazonSQS M2Wsqs;
    
    static final String REGION = "us-west-2";
    static final String L2M_QUEUE = "LocalAppToManager";
    static final String W2M_QUEUE = "WorkerToManager";
    static final String M2W_QUEUE = "ManagerToWorker";
    
    static final String M2W_VISIBILITY_TIMEOUT = "300";
    
    static String M2WUrl;
    
    private static QueueConnection connectionToLocalApp;
    private static QueueConnection connectionToWorkers;
    
    static ExecutorService downloadPool;
    static ExecutorService distributionPool;
    static ExecutorService nodesCreationPool;
    static ExecutorService fileEditionPool;
    static final int DOWNLOAD_THREADS = 5;
    static final int DISTRIBUTION_THREADS = 5;
    static final int CREATION_THREADS = 1;
    static final int EDITION_THREADS = 5;
    
    static FileManager fileManager;
    
    public static void main(String[] args) {
        
        // Connect to the AWS services
        initialize();
        
        // Create SQS queues if not exist
        SQSHelper.createSQSQueue(M2Lsqs, L2M_QUEUE);
        SQSHelper.createSQSQueue(M2Wsqs, W2M_QUEUE);
        M2WUrl = SQSHelper.createSQSQueue(M2Wsqs, M2W_QUEUE, M2W_VISIBILITY_TIMEOUT);
        
        // Create SQS connections to queues
        connectionToLocalApp = new QueueConnection(
                REGION, credentialsProvider, L2M_QUEUE, new ManagerListenerToLocalApp());
        connectionToWorkers = new QueueConnection(
                REGION, credentialsProvider, W2M_QUEUE, new ManagerListenerToWorkers());
        
        // Start listening to queues
        connectionToLocalApp.start();
        connectionToWorkers.start();
        
        // Monitor workers to ensure enough worker nodes are running
        new Thread(new MonitorWorkers()).start();
        
        // Main thread go to sleep forever
        Object sleepForever = new Object();
        while (true) {
            synchronized (sleepForever) {
                try {
                    sleepForever.wait();
                } catch (InterruptedException ignored) {}
            }
        }
    }

    private static void initialize() {

        // Get credentials
        credentialsProvider = new InstanceProfileCredentialsProvider(false);
        credentialsProvider.getCredentials();

        // Connect to S3 cloud
        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(REGION)
                .build();

        // Connect to SQS Service
        M2Wsqs = AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(REGION)
                .build();
        M2Lsqs = AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(REGION)
                .build();

        // Connect to EC2 cloud
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(REGION)
                .build();
        
        // Create thread pools for manager's tasks
        downloadPool = Executors.newFixedThreadPool(DOWNLOAD_THREADS);
        distributionPool = Executors.newFixedThreadPool(DISTRIBUTION_THREADS);
        nodesCreationPool = Executors.newFixedThreadPool(CREATION_THREADS);
        fileEditionPool = Executors.newFixedThreadPool(EDITION_THREADS);
        
        // Create file manager
        fileManager = new FileManager();
            
    }
    
}
