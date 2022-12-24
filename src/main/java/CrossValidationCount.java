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

public class CrossValidationCount {

    public static class MapperClass extends Mapper<LongWritable, Text,
            PairWritable<IntWritable, IntWritable>,
            PairWritable<IntWritable, Text>> {
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
        private String line;
        private String[] words;
        private Text triGram = new Text("");
        private IntWritable groupZeroCount = zero;
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
            // TODO: refactor comments
            //first group count (the index of the group is 0), the value is the 3gram with the second group count
            context.write(new PairWritable<>(groupZeroCount, zero), new PairWritable(groupOneCount, triGram));
            //second group count (the index of the group is 1), the value is the 3gram with the first group count
            context.write(new PairWritable<>(groupOneCount, one), new PairWritable(groupZeroCount, triGram));
        }
    }

    public static class ReducerClass extends Reducer<PairWritable<IntWritable, IntWritable>, PairWritable<IntWritable, Text>,
            Text,PairWritable<IntWritable, Text>> {

        @Override
        public void reduce(PairWritable<IntWritable, IntWritable> key, Iterable<PairWritable<IntWritable, Text>> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            String tag;
            List<String> ngrams = new LinkedList<>();
            for (PairWritable pair : values) {
                if(!(ngrams.contains(pair.second.toString()))){
                    ngrams.add(pair.second().toString());
                }

                sum += ((IntWritable)pair.first).get();
            }

            if(key.second.get() == 0) {
                tag = "TR01";
            } else {
                tag = "TR10";
            }

            for (String ngram : ngrams) {
                context.write(new Text(ngram), new PairWritable<>(new IntWritable(sum), new Text(tag)));
            }
        }

    }

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        //String log4jConfPath = "C:/hadoop-2.8.0/etc/hadoop/log4j.properties";
        //PropertyConfigurator.configure(log4jConfPath);

        Configuration conf = new Configuration();
        Job job = new Job(conf, "CrossValidation");
        job.setJarByClass(CrossValidationCount.class);

        job.setMapOutputKeyClass(PairIntIntWritable.class);
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
