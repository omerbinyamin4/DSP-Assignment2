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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class ParamsMR {

    public static class MapperClass extends Mapper<LongWritable, Text,
            PairTxtInt, PairTxtInt> {
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
        private Text triGram = new Text("");
        private IntWritable groupZeroCount= zero;
        private IntWritable groupOneCount = zero;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // line = <trigram> <groupZeroCount> <groupOneCount>
            String[] words = line.split("\t");
            if(words[0] != null)
                triGram.set(words[0]);
            if(words[1] != null)
                groupZeroCount.set(Integer.parseInt(words[1]));
            if(words[2] != null)
                groupOneCount.set(Integer.parseInt(words[2]));

            // NR values definitions
            PairTxtInt triGramKey = new PairTxtInt(triGram, one);
            // summing up these lines later will produce nr0 of all r values.  groupZeroCount = r , each triGram has value of 1 for counting purposes
            context.write(new PairTxtInt(new Text("NR0"), groupZeroCount), triGramKey);
            // summing up these lines later will produce nr1 of all r values.  groupOneCount = r , each triGram has value of 1 for counting purposes
            context.write(new PairTxtInt(new Text("NR1"), groupOneCount), triGramKey);

            // TR values definitions
            // summing up these lines later will produce tr01 of all r values.  groupZeroCount = r , each value has the triGram itself and number of its occurrences in the opposite group (group one)
            context.write(new PairTxtInt(new Text("TR01"), groupZeroCount), new PairTxtInt(triGram, groupOneCount));
            // summing up these lines later will produce tr10 of all r values.  groupOneCount = r , each value has the triGram itself and number of its occurrences in the opposite group (group zero)
            context.write(new PairTxtInt(new Text("TR10"), groupOneCount), new PairTxtInt(triGram, groupZeroCount));
        }
    }

    public static class ReducerClass extends Reducer<PairTxtInt, PairTxtInt,
            Text, PairTxtInt> {

        @Override
        public void reduce(PairTxtInt key, Iterable<PairTxtInt> values, Context context) throws IOException,  InterruptedException {
            // valueSum is for summing up the counts for each nr/tr value (according to the key text)
            int valueSum = 0;
            List<String> triGrams = new LinkedList<>();
            // each pair in values is the either a trigram and his count for nr values or the trigram and the count of the opposite group (for tr values)
            for (PairTxtInt pair : values) {
                if(!(triGrams.contains(pair.first().toString()))){
                    triGrams.add(pair.first().toString());
                }
                valueSum += pair.second().get();
            }

            for (String triGram : triGrams) {
                // for each triGram we write the triGram as Key, and the parameter name (nr0/nr1/tr01/tr10) and it's summed up value as value.
                context.write(new Text(triGram), new PairTxtInt(key.first(), new IntWritable(valueSum)));
            }
        }
    }

    //TODO: refactor to updated version of Hadoop
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //String log4jConfPath = "C:/hadoop-2.8.0/etc/hadoop/log4j.properties";
        //PropertyConfigurator.configure(log4jConfPath);

        Configuration conf = new Configuration();
        Job job = new Job(conf, "ParamsMR");
        job.setJarByClass(ParamsMR.class);

        job.setMapOutputKeyClass(PairTxtInt.class);
        job.setMapOutputValueClass(PairTxtInt.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairTxtInt.class);

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
