import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class RAndCrossValCounts {

    public static class MapperClass extends Mapper<LongWritable, Text,
            PairWritable<IntWritable, Text>, PairWritable<IntWritable, Text>> {
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
        private String line;
        private String[] words;
        private Text triGram = new Text("");
        private IntWritable groupZeroCount= zero;
        private IntWritable groupOneCount = zero;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            line = value.toString();
            words = line.split("\t");
            if(words[0] != null)
                triGram.set(words[0]);
            if(words[1] != null)
                groupZeroCount.set(Integer.valueOf(words[1]));
            if(words[2] != null)
                groupOneCount.set(Integer.valueOf(words[2]));

            //TODO: refactor comments
            //==========================RCount================================//
            PairWritable<IntWritable, Text> triGramKey = new PairWritable(one, triGram);
            //first group count (the index of the group is 0), the value is the 3gram with its count(1 time ectually)
            context.write(new PairWritable(groupZeroCount, new Text("NR0")), triGramKey);
            //second group count (the index of the group is 1), the value is the 3gram with its count(1 time ectually)
            context.write(new PairWritable(groupOneCount, new Text("NR1")), triGramKey);

            //=====================CrossValidationCount=======================//
            //first group count (the index of the group is 0), the value is the 3gram with the second group count
            context.write(new PairWritable(groupZeroCount, new Text("TR0")), new PairWritable(groupOneCount, triGram));
            //second group count (the index of the group is 1), the value is the 3gram with the first group count
            context.write(new PairWritable(groupOneCount, new Text("TR1")), new PairWritable(groupZeroCount, triGram));
        }
    }

    public static class ReducerClass extends Reducer<PairWritable<IntWritable, Text>, PairWritable<IntWritable, Text>,
            Text,PairWritable<IntWritable, Text>> {

        @Override
        public void reduce(PairWritable<IntWritable, Text> key, Iterable<PairWritable<IntWritable, Text>> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            List<String> triGrams = new LinkedList<>();
            for (PairWritable<IntWritable, Text> pair : values) {
                if(!(triGrams.contains(pair.second.toString()))){
                    triGrams.add(pair.second().toString());
                }
                sum += pair.first.get();
            }

            for (String triGram : triGrams) {
                context.write(new Text(triGram), new PairWritable(new IntWritable(sum), key.second));
            }
        }
    }

    //TODO: refactor to updated version of Hadoop
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        //String log4jConfPath = "C:/hadoop-2.8.0/etc/hadoop/log4j.properties";
        //PropertyConfigurator.configure(log4jConfPath);

        Configuration conf = new Configuration();
        Job job = new Job(conf, "RAndCrossValCounts");
        job.setJarByClass(RAndCrossValCounts.class);

        job.setMapOutputKeyClass(PairIntTextWritable.class);
        job.setMapOutputValueClass(PairIntTextWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairIntTextWritable.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //job.setNumReduceTasks(1);

        int argsLength = args.length;
        FileInputFormat.addInputPath(job, new Path(args[argsLength-2]));
        FileOutputFormat.setOutputPath(job, new Path(args[argsLength-1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
