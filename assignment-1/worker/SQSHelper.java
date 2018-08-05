package dsps.ass1.worker;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.QueueNameExistsException;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SQSHelper {
    
    private static final String DEFAULT_VISIBILITY_TIMEOUT = "30";
		
    public static String createSQSQueue(AmazonSQS sqs, String sqsQueueName) {
        return createSQSQueue(sqs, sqsQueueName, DEFAULT_VISIBILITY_TIMEOUT);
    }

    public static void sendSQSMessage(AmazonSQS sqs, String queueUrl, String message) {
        try {
            sqs.sendMessage(new SendMessageRequest(queueUrl, message));
        } catch (AmazonClientException ex) {
            System.err.println("Caught an exception while sending a message to SQS queue");
            throw ex;
        }
        System.out.println("Message sent to SQS. sqs url:"+queueUrl+" queue:\n" + message);
    }

    public static String createSQSQueue(AmazonSQS sqs, String sqsQueueName, String visibilityTimeout) {
        String ret;
        try {
            ret = sqs.createQueue(new CreateQueueRequest(sqsQueueName)
                    .addAttributesEntry(QueueAttributeName.VisibilityTimeout.toString(),
                            visibilityTimeout))
                    .getQueueUrl();
            System.out.println("SQS queue created: " + ret);
        } catch (QueueNameExistsException ex) {
            ret = sqs.getQueueUrl(sqsQueueName).getQueueUrl();
            System.out.println("SQS queue already exists: " + ret);
        } catch (AmazonClientException ex) {
            System.err.println("Caught an exception while creating SQS queue: " + sqsQueueName);
            throw ex;
        }
        return ret;
    }
}
