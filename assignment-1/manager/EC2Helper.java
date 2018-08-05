package dsps.ass1.manager;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateName;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.ResourceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TagSpecification;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

public class EC2Helper {
    private static final String PROCESS_KEY = "Process";
    private static final String WORKER_PROCESS_VALUE = "Worker";
    private static final String WORKER_IMAGE_ID = "ami-0cd30a9488e8e7c65";
    // worker roll
    private static final String WORKER_ARN = "arn:aws:iam::534542618717:instance-profile/Worker";
    private static final String WORKER_USER_DATA_SCRIPT = 
            "#!/bin/bash\n" +
            "cd home/ec2-user/\n" +
            "java -jar worker.jar\n" + 
            "ec2-terminate-instances $(curl -s http://169.254.169.254/latest/meta-data/instance-id)\n";
    
    public static void createWorkers(AmazonEC2 ec2, int numOfWorkers) {
        try {
            ec2.runInstances(new RunInstancesRequest()
                    .withImageId(WORKER_IMAGE_ID)
                    .withInstanceType(InstanceType.T2Micro)
                    .withMinCount(numOfWorkers)
                    .withMaxCount(numOfWorkers)
                    .withUserData(Base64.getEncoder()
                            .encodeToString(WORKER_USER_DATA_SCRIPT.getBytes()))
                    .withIamInstanceProfile(new IamInstanceProfileSpecification()
                            .withArn(WORKER_ARN)
                            )
                    .withTagSpecifications(new TagSpecification()
                            .withResourceType(ResourceType.Instance)
                            .withTags(new Tag()
                                    .withKey(PROCESS_KEY)
                                    .withValue(WORKER_PROCESS_VALUE))));
        } catch (AmazonClientException ex) {
            System.err.println("Caught an exception while creating worker instances");
            ex.printStackTrace();
        }
        System.out.printf("%d Worker instances created and started\n", numOfWorkers);
    }
    
    public static void terminateInstance(AmazonEC2 ec2, Instance instance) {
        try {
            ec2.terminateInstances(new TerminateInstancesRequest()
                    .withInstanceIds(instance.getInstanceId()));
        } catch (AmazonClientException ex) {
            System.err.println("Caught an exception while trying to terminate instance");
            ex.printStackTrace();
        }
        System.out.println("Terminated EC2 instance");
    }
    
    public static void terminateAllWorkers(AmazonEC2 ec2) {
        try {
            Collection<String> instanceIds = getWorkerIds(ec2);
            TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest()
                    .withInstanceIds(instanceIds);
            ec2.terminateInstances(terminateInstancesRequest);
        } catch (AmazonClientException ex) {
            System.err.println("Caught an exception while trying to terminate workers");
            ex.printStackTrace();
        }
        System.out.println("Terminated all workers");
    }

    private static Collection<String> getWorkerIds(AmazonEC2 ec2) throws AmazonClientException {
        Collection<String> res = new ArrayList<String>();
        for (Reservation reservation : ec2.describeInstances().getReservations()) {
            for (Instance instance : reservation.getInstances()) {
                for (Tag tag : instance.getTags()) {
                    if (tag.getKey().equals(PROCESS_KEY) &&
                            tag.getValue().equals(WORKER_PROCESS_VALUE) &&
                            !instance.getState().getName().
                            equals(InstanceStateName.ShuttingDown.toString()) &&
                            !instance.getState().getName().
                            equals(InstanceStateName.Terminated.toString())) {
                        res.add(instance.getInstanceId());
                    }
                }
            }
        }
        return res;
    }

    public static int countWorkers(AmazonEC2 ec2) {
        int count = 0;
        try {
            count = getWorkerIds(ec2).size();
        } catch (AmazonClientException ex) {
            System.err.println("Caught an exception while counting worker instances");
            ex.printStackTrace();
        }
        return count;
    }

}
