import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

public class Start {

    private static final String EMR_RELEASE_LABEL = "emr-5.16.0";
    private static final String HADOOP_VERSION = "2.8.4";
    private static final String EC2_KEY_NAME = "dsps-ass1";
    private static final String REGION = "us-west-2";
    private static final String SUBNET_ID = "subnet-0df26357";
    private static final int NUM_OF_INSTANCES = 20;
    private static final String INSTANCE_TYPE = InstanceType.M4Large.toString();

    private static final String BUCKET_NAME = "assignment2-dsps-2";
    private static final String BUCKET_URL = "s3n://"+BUCKET_NAME+"/";
    private static final String LOG_DIR = BUCKET_URL + "logs/";

    private static final String ONE_GRAM_URL_HEB = "s3://datasets.elasticmapreduce/"
            + "ngrams/books/20090715/heb-all/1gram/data";
    private static final String TWO_GRAM_URL_HEB = "s3://datasets.elasticmapreduce/"
            + "ngrams/books/20090715/heb-all/2gram/data";
    private static final String ONE_GRAM_URL_ENG = "s3://datasets.elasticmapreduce/"
            + "ngrams/books/20090715/eng-us-all/1gram/data";
    private static final String TWO_GRAM_URL_ENG = "s3://datasets.elasticmapreduce/"
            + "ngrams/books/20090715/eng-us-all/2gram/data";

    private static final String STEP1_JAR = BUCKET_URL + "jars/step1.jar";
    private static final String STEP2_JAR = BUCKET_URL + "jars/step2.jar";
    private static final String STEP3_JAR = BUCKET_URL + "jars/step3.jar";

    private static final String STEP1_OUTPUT = BUCKET_URL + "step1output/";
    private static final String STEP2_OUTPUT = BUCKET_URL + "step2output/";
    private static final String STEP3_OUTPUT = BUCKET_URL + "step3output/";

    public static void main(String[] args) {

        AmazonElasticMapReduce mapReduce =
                AmazonElasticMapReduceClientBuilder
                        .standard()
                        .withRegion(REGION)
                        .build();
        System.out.println("Connected to EMR");

        // set default language to hebrew and default local aggregation to false
        String language = args.length == 0 ? "heb" : args[0];
        String localAggregation = args.length <= 1 ? "false" : args[1];
        String oneGramURL, twoGramURL;

        if (language.equals("eng")) {
            oneGramURL = ONE_GRAM_URL_ENG;
            twoGramURL = TWO_GRAM_URL_ENG;
        } else {
            oneGramURL = ONE_GRAM_URL_HEB;
            twoGramURL = TWO_GRAM_URL_HEB;
        }

        // Configure step 0 to enable debugging
        StepConfig stepConfigDebug = new StepConfig()
                .withName("Enable Debugging")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(new HadoopJarStepConfig()
                        .withJar("command-runner.jar")
                        .withArgs("state-pusher-script"));

        // Configure first step, input is 1-gram, 2-gram
        StepConfig stepConfig1 = new StepConfig()
                .withName("step1")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(new HadoopJarStepConfig()
                        .withJar(STEP1_JAR)
                        .withArgs(language, oneGramURL, twoGramURL, STEP1_OUTPUT, localAggregation));

        // Configure second step, input is the output of first step
        StepConfig stepConfig2 = new StepConfig()
                .withName("step2")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(new HadoopJarStepConfig()
                        .withJar(STEP2_JAR)
                        .withArgs(STEP1_OUTPUT,STEP2_OUTPUT));

        // Configure third step, input is the output of second step
        StepConfig stepConfig3 = new StepConfig()
                .withName("step3")
                .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                .withHadoopJarStep(new HadoopJarStepConfig()
                        .withJar(STEP3_JAR)
                        .withArgs(STEP2_OUTPUT,STEP3_OUTPUT));

        System.out.println("Configured all steps");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(NUM_OF_INSTANCES)
                .withMasterInstanceType(INSTANCE_TYPE)
                .withSlaveInstanceType(INSTANCE_TYPE)
                .withHadoopVersion(HADOOP_VERSION)
                .withEc2KeyName(EC2_KEY_NAME)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withEc2SubnetId(SUBNET_ID);

        // Create a flow request including all the steps
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("start")
                .withInstances(instances)
                .withSteps(stepConfigDebug,stepConfig1,stepConfig2,stepConfig3)
                .withLogUri(LOG_DIR)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel(EMR_RELEASE_LABEL);

        System.out.println("Created job flow request");

        // Run the flow
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
