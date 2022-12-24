
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import software.amazon.awssdk.services.s3.endpoints.internal.Value;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;


public class TriGramsMR {
    public enum N{
        Count
    };

    public static class MapperClass extends Mapper<LongWritable, Text, Text, PairWritable<IntWritable,IntWritable>> {
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
        private Text triGram = new Text("");
        private String[] words;
        private String line;


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            line = value.toString();
            words = line.split("\t");
            if(words.length<1)
                return;
            triGram.set(words[0]);
            int currNGramLength = words[0].split(" ").length;
            if(currNGramLength < 3 || containsStopWords(words[0]))
                return;

            //TODO: how can be refactored?
            context.getCounter(N.Count).increment(1);

            int corpusPartitionGroup = (int)(Math.random() * 2);
            if (corpusPartitionGroup == 0){
                context.write(triGram, new PairWritable<>(one, zero));
            }
            else{
                context.write(triGram, new PairWritable<>(zero, one));
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,PairWritable<IntWritable, IntWritable>,Text,PairWritable<IntWritable, IntWritable>> {


        @Override
        public void reduce(Text key, Iterable<PairWritable<IntWritable, IntWritable>> values, Context context) throws IOException,  InterruptedException {
            int groupZeroCount = 0;
            int groupOneCount = 0;
            for (PairWritable<IntWritable, IntWritable> value : values) {
                groupZeroCount += value.first.get();
                groupOneCount += value.second.get();
            }
            context.write(key, new PairWritable<>(new IntWritable(groupZeroCount), new IntWritable(groupOneCount)));        }
    }

    //TODO: refactor to updated version of hadoop
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        //String log4jConfPath = "G:/hadoop-2.6.2/etc/hadoop/log4j.properties";
        //PropertyConfigurator.configure(log4jConfPath);

        Configuration conf = new Configuration();
        int argsLength = args.length;

        String myBucketname = args[argsLength-3];
        conf.set("bucketname", myBucketname);

        Job job = new Job(conf, "TriGramsCount");
        job.setJarByClass(TriGramsMR.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairWritable.class);

        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);


        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[argsLength-2]));
        FileOutputFormat.setOutputPath(job, new Path(args[argsLength-1]));

        boolean end = job.waitForCompletion(true);
        writeN(conf, myBucketname, job);
        System.exit(end ? 0 : 1);



    }

    //TODO: refactor to updated version of hadoop
    public static void writeN(Configuration conf, String bucketname, Job job)  throws IOException, InterruptedException {
        long n = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        FileSystem fileSystem = FileSystem.get(URI.create("s3://" + bucketname), conf);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("s3://" + bucketname + "/N.txt"));
        PrintWriter writer  = new PrintWriter(fsDataOutputStream);
        writer.write(String.valueOf(n));
        writer.close();
        fsDataOutputStream.close();

    }

    public static boolean containsStopWords(String triGram) throws FileNotFoundException {
        List<String> wordsList = new ArrayList<>(Arrays.asList(triGram.split("")));
        try {
            Scanner myReader = new Scanner(new File("src/main/java/eng-stopwords.txt"));
            while (myReader.hasNextLine()) {
                String stopWord = myReader.nextLine();
                if (wordsList.contains(stopWord)) {
                    return true;
                }
            }

        }
        catch (FileNotFoundException e){
            System.out.println(e.getMessage());
        }
        return false;
    }

}
