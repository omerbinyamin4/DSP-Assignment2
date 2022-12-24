import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class SortResults {

    public static class MapperClass extends Mapper<LongWritable, Text, PairWritable<Text, DoubleWritable>, Text> {

        private String[] words;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] triGramWordsAndScore = value.toString().split("\t");
            String[] triGramWords = triGramWordsAndScore[0].split(" ");
            String firstWord = triGramWords[0];
            String secondWord = triGramWords[1];
            String thirdWord = triGramWords[2];

            DoubleWritable probabilityScore = new DoubleWritable(Double.valueOf(triGramWordsAndScore[1]));
            context.write(new PairWritable<>(new Text(firstWord + " " + secondWord), probabilityScore), new Text(thirdWord));
        }
    }

    public static class ReducerClass extends Reducer<PairWritable<Text, DoubleWritable>, Text, Text, DoubleWritable> {

        @Override
        public void reduce(PairWritable<Text, DoubleWritable> key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            for(Text thirdWord : values) {
                context.write(new Text(key.first.toString() + " " + thirdWord.toString()), key.second);
            }
        }

    }





    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        //String log4jConfPath = "G:/hadoop-2.6.2/etc/hadoop/log4j.properties";
        //PropertyConfigurator.configure(log4jConfPath);

        Configuration conf = new Configuration();
        Job job = new Job(conf, "Sort");
        job.setJarByClass(SortResults.class);

        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        job.setNumReduceTasks(1);

        int argsLength = args.length;
        FileInputFormat.addInputPath(job, new Path(args[argsLength-2]));
        FileOutputFormat.setOutputPath(job, new Path(args[argsLength-1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
