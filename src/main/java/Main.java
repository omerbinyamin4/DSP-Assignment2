import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.emr.*;
import software.amazon.awssdk.services.emr.model.*;


import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class Main {

    private static final String myBucketName = "dsp2-hadoop";
    private static final String TriGramsCount = "TriGramsMR";
    private static final String ParamsMR = "ParamsMR";
//    private static final String myJarName = "ass2LA.jar"; // JAR with combiner
    private static final String myJarName = "ass2.jar"; // JAR without combiner
    private static final String ProbCalc = "ProbCalc";
    private static final String SortResults = "SortResults";


    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        EmrClient emrClient = EmrClient.builder()
                .region(Region.US_EAST_1)
                .build();

        List<StepConfig> steps = new LinkedList<StepConfig>();

        // ======================Step 1===========================
        String input = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data";

        HadoopJarStepConfig TriGramsStep = HadoopJarStepConfig.builder()
                .args(myBucketName, input, "s3n://" + myBucketName + "/output1NLA/")
                .jar("s3://" + myBucketName + "/" + myJarName)
                .mainClass("TriGramsMR")
                .build();
        StepConfig TriGramsStepConf = StepConfig.builder()
                .name("hadoop-" + TriGramsCount)
                .hadoopJarStep(TriGramsStep)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
        // ======================Step 2===========================
        HadoopJarStepConfig ParamsMRStep = HadoopJarStepConfig.builder()
                .args("s3n://" + myBucketName + "/output1NLA/", "s3n://" + myBucketName + "/output2NLA/")
                .jar("s3://" + myBucketName + "/" + myJarName)
                .mainClass("ParamsMR")
                .build();
        StepConfig ParamsMRConf = StepConfig.builder()
                .name("hadoop-" + ParamsMR)
                .hadoopJarStep(ParamsMRStep)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

//        // ======================Step 3===========================
        HadoopJarStepConfig ProbCalcStep = HadoopJarStepConfig.builder()
                .args(myBucketName, "s3n://" + myBucketName + "/output2NLA/", "s3n://" + myBucketName + "/output3NLA/")
                .jar("s3://" + myBucketName + "/" + myJarName)
                .mainClass("ProbCalc")
                .build();
        StepConfig ProbCalcConf = StepConfig.builder()
                .name("hadoop-" + ProbCalc)
                .hadoopJarStep(ProbCalcStep)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

//        ======================Step 4===========================
        HadoopJarStepConfig SortResultsStep = HadoopJarStepConfig.builder()
                .args("s3n://" + myBucketName + "/output3NLA/", "s3n://" + myBucketName + "/output4NLA/")
                .jar("s3://" + myBucketName + "/" + myJarName)
                .mainClass("SortResults")
                .build();
        StepConfig SortResultsConf = StepConfig.builder()
                .name("hadoop-" + SortResults)
                .hadoopJarStep(SortResultsStep)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();


        steps.add(TriGramsStepConf);
        steps.add(ParamsMRConf);
        steps.add(ProbCalcConf);
        steps.add(SortResultsConf);

        JobFlowInstancesConfig instances = JobFlowInstancesConfig.builder()
                .instanceCount(6)
                .masterInstanceType(InstanceType.M4_LARGE.toString())
                .slaveInstanceType(InstanceType.M4_LARGE.toString())
                .hadoopVersion("3.4.4")
                .keepJobFlowAliveWhenNoSteps(false)
                .placement(PlacementType.builder().availabilityZone("us-east-1a").build())
                .build();


        RunJobFlowRequest runFlowRequest = RunJobFlowRequest.builder()
                .releaseLabel("emr-5.2.0")
                .name("hadoop-ass2")
                .instances(instances)
                .steps(steps)
                .logUri("s3://" + myBucketName + "/")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .serviceRole("EMR_DefaultRole")
                .build();

        RunJobFlowResponse runJobFlowResponse = emrClient.runJobFlow(runFlowRequest);

        String jobFlowId = runJobFlowResponse.jobFlowId();
        System.out.println("Job Flow Started: " + jobFlowId);
    }

}



