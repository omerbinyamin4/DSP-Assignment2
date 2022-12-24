import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.emr.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.log4j.PropertyConfigurator;
import software.amazon.awssdk.services.emr.model.*;
import software.amazon.awssdk.core.traits.*;
import com.amazonaws.*;


import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class Main {

    private static final String myBucketName = "dsp2-hadoop";
    private static final String TriGramsCount = "TriGramsMR";
    private static final String ParamsMR = "ParamsMR";
    private static final String myJarName = "ass2LA.jar";
//    private static final String myJarName = "ass2jar.jar";
    private static final String ProbCalc = "ProbCalc";
    private static final String SortResults = "SortResults";
    private static final String myKeyPair = "arnon";


    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
//        String log4jConfPath = "C:/hadoop-2.8.0/etc/hadoop/log4j.properties";
//        PropertyConfigurator.configure(log4jConfPath);

        EmrClient emrClient = EmrClient.builder()
                .region(Region.US_EAST_1)
                .build();

        List<StepConfig> steps = new LinkedList<StepConfig>();

        // ======================Step 1===========================
        String GoogleEnglish3Grams = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data";
        HadoopJarStepConfig TriGramsStep = HadoopJarStepConfig.builder()
                .args(myBucketName, GoogleEnglish3Grams, "s3n://" + myBucketName + "/output1/")
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
                .args("s3n://" + myBucketName + "/output1/", "s3n://" + myBucketName + "/output2/")
                .jar("s3://" + myBucketName + "/" + myJarName)
                .mainClass("ParamsMR")
                .build();
        StepConfig ParamsMRConf = StepConfig.builder()
                .name("hadoop-" + ParamsMR)
                .hadoopJarStep(ParamsMRStep)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
//
//        // ======================Step 3===========================
        HadoopJarStepConfig ProbCalcStep = HadoopJarStepConfig.builder()
                .args(myBucketName, "s3n://" + myBucketName + "/output2/", "s3n://" + myBucketName + "/output3/")
                .jar("s3://" + myBucketName + "/" + myJarName)
                .mainClass("ProbCalc")
                .build();
        StepConfig ProbCalcConf = StepConfig.builder()
                .name("hadoop-" + ProbCalc)
                .hadoopJarStep(ProbCalcStep)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
//
//
//        // ======================Step 4===========================
        HadoopJarStepConfig SortResultsStep = HadoopJarStepConfig.builder()
                .args("s3n://" + myBucketName + "/output3/", "s3n://" + myBucketName + "/output4/")
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
                .instanceCount(4)
                .masterInstanceType(InstanceType.M4_LARGE.toString())
                .slaveInstanceType(InstanceType.M4_LARGE.toString())
                .hadoopVersion("3.4.4").ec2KeyName(myKeyPair)
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



