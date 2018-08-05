package dsps.ass1.localapp;

import java.util.Base64;

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

public class EC2Helper {
    private static final String PROCESS_KEY = "Process";
    private static final String MANAGER_PROCESS_VALUE = "Manager";
    private static final String MANAGER_IMAGE_ID = "ami-019d287d1fae6ecdb";
    // manager roll
    private static final String MANAGER_ARN = "arn:aws:iam::534542618717:instance-profile/Manager";
    private static final String MANAGER_USER_DATA_SCRIPT = 
            "#!/bin/bash\n" +
            "cd home/ec2-user/\n" +
            "java -jar manager.jar\n";
    
    public static void startManagerIfNotActive(AmazonEC2 ec2) {
        try {
            Instance manager = findManager(ec2);
            if (manager == null) {
                createManager(ec2);
                System.out.println("Manager instance created and started");
            } else {
                System.out.println("Manager instance is already running");
            }
        } catch (AmazonClientException ex) {
            System.err.println("Caught an exception while creating the ec2 manager node");
            throw ex;
        }
    }

    private static void createManager(AmazonEC2 ec2) throws AmazonClientException {
        ec2.runInstances(new RunInstancesRequest()
                .withImageId(MANAGER_IMAGE_ID)
                .withInstanceType(InstanceType.T2Micro)
                .withMinCount(1)
                .withMaxCount(1)
                .withUserData(Base64.getEncoder()
                        .encodeToString(MANAGER_USER_DATA_SCRIPT.getBytes()))
                .withIamInstanceProfile(new IamInstanceProfileSpecification()
                        .withArn(MANAGER_ARN)
                        )
                .withTagSpecifications(new TagSpecification()
                        .withResourceType(ResourceType.Instance)
                        .withTags(new Tag()
                                .withKey(PROCESS_KEY)
                                .withValue(MANAGER_PROCESS_VALUE))));
    }

    private static Instance findManager(AmazonEC2 ec2) {
        for (Reservation reservation : ec2.describeInstances().getReservations()) {
            for (Instance instance : reservation.getInstances()) {
                for (Tag tag : instance.getTags()) {
                	// checking for running instance with manager tag
                    if (tag.getKey().equals(PROCESS_KEY) &&
                            tag.getValue().equals(MANAGER_PROCESS_VALUE) &&
                            (instance.getState().getName().
                            equals(InstanceStateName.Pending.toString()) ||
                            instance.getState().getName().
                            equals(InstanceStateName.Running.toString()))) {
                        return instance;
                    }
                }
            }
        }
        return null;
    }
	
}
