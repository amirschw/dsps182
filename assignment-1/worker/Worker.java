package dsps.ass1.worker;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.util.EC2MetadataUtils;

public class Worker {
	
	static final String W2M_QUEUE = "WorkerToManager";
	static final String M2W_QUEUE = "ManagerToWorker";
	static final String REGION = "us-west-2";
	
    static InstanceProfileCredentialsProvider credentialsProvider;
	static final Object lock = new Object();
	static AmazonSQS sqs;
	
	private static QueueConnection connection;
	static String W2MUrl;
	static String instanceId;
	
	public static void main(String[] args) {
		try {
			instanceId = EC2MetadataUtils.getInstanceId();
			System.out.println("Worker is started. instance id: " + instanceId);
			
			init();
			
			// start listen to manager messages
			connection.start();
			
			// main thread go to sleep forever
			Object sleepForever = new Object();
			while (true) {
	            synchronized (sleepForever) {
	                try {
	                    sleepForever.wait();
	                } catch (InterruptedException ignored) {}
	            }
			}
		} catch (Exception e) {
			System.err.println("Caught an exception while trying to initialize:\n" + e);
			System.exit(1);
		}
	}
	
    public static void init() throws Exception {
    	
        credentialsProvider = new InstanceProfileCredentialsProvider(false);
        credentialsProvider.getCredentials();
        
        System.out.println("Succesfully recieved credentials");
        
        // connect to sqs
        sqs = AmazonSQSClientBuilder.standard().withCredentials(credentialsProvider)
        		.withRegion(REGION).build();
    	
        // get WorkerToManager sqs url
        W2MUrl = sqs.getQueueUrl(W2M_QUEUE).getQueueUrl();
    	
        // create connection to listen to ManagerToWorker sqs
        connection = new QueueConnection(
        		REGION,
        		credentialsProvider,
        		M2W_QUEUE,
        		new WorkerListener()
        		);
    	
    	MyOcr.init();
    	System.out.println("Succesfully initialized MyOcr"); 
        
	}

}
