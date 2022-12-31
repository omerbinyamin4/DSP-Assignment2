
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class TriGramsMR {
    public enum N{
        Count
    };

    public static class MapperClass extends Mapper<LongWritable, Text, Text, PairIntInt> {

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
            if(currNGramLength < 3 || containsStopWords(words[0], context.getConfiguration())){
                return;
            }


            //TODO: how can be refactored?
            context.getCounter(N.Count).increment(1);

            int corpusPartitionGroup = (int)(Math.random() * 2);
            if (corpusPartitionGroup == 0){
                context.write(new Text(words[0]), new PairIntInt(one, zero));
            }
            else{
                context.write(new Text(words[0]), new PairIntInt(zero, one));
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,PairIntInt,Text,PairIntInt> {


        @Override
        public void reduce(Text key, Iterable<PairIntInt> values, Context context) throws IOException,  InterruptedException {
            int groupZeroCount = 0;
            int groupOneCount = 0;
            for (PairIntInt value : values) {
                groupZeroCount += value.first().get();
                groupOneCount += value.second().get();
            }
            context.write(key, new PairIntInt(new IntWritable(groupZeroCount), new IntWritable(groupOneCount)));        }
    }

    //TODO: refactor to updated version of hadoop
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        int argsLength = args.length;

        String myBucketname = args[argsLength-3];
        conf.set("bucketname", myBucketname);


        Job job = new Job(conf, "TriGramsCount");
        job.setJarByClass(TriGramsMR.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairIntInt.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairIntInt.class);

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
    private static void writeN(Configuration conf, String bucketname, Job job)  throws IOException, InterruptedException {
        long n = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        FileSystem fileSystem = FileSystem.get(URI.create("s3://" + bucketname), conf);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("s3://" + bucketname + "/N.txt"));
        PrintWriter writer  = new PrintWriter(fsDataOutputStream);
        writer.write(String.valueOf(n));
        writer.close();
        fsDataOutputStream.close();

    }

    private static boolean containsStopWords(String triGram, Configuration conf){
        List<String> wordsList = new ArrayList<>(Arrays.asList(triGram.split("")));
        String stopWords =
                "a\n" +
                "about\n" +
                "above\n" +
                "across\n" +
                "after\n" +
                "afterwards\n" +
                "again\n" +
                "against\n" +
                "all\n" +
                "almost\n" +
                "alone\n" +
                "along\n" +
                "already\n" +
                "also\n" +
                "although\n" +
                "always\n" +
                "am\n" +
                "among\n" +
                "amongst\n" +
                "amoungst\n" +
                "amount\n" +
                "an\n" +
                "and\n" +
                "another\n" +
                "any\n" +
                "anyhow\n" +
                "anyone\n" +
                "anything\n" +
                "anyway\n" +
                "anywhere\n" +
                "are\n" +
                "around\n" +
                "as\n" +
                "at\n" +
                "back\n" +
                "be\n" +
                "became\n" +
                "because\n" +
                "become\n" +
                "becomes\n" +
                "becoming\n" +
                "been\n" +
                "before\n" +
                "beforehand\n" +
                "behind\n" +
                "being\n" +
                "below\n" +
                "beside\n" +
                "besides\n" +
                "between\n" +
                "beyond\n" +
                "bill\n" +
                "both\n" +
                "bottom\n" +
                "but\n" +
                "by\n" +
                "call\n" +
                "can\n" +
                "cannot\n" +
                "cant\n" +
                "co\n" +
                "computer\n" +
                "con\n" +
                "could\n" +
                "couldnt\n" +
                "cry\n" +
                "de\n" +
                "describe\n" +
                "detail\n" +
                "do\n" +
                "done\n" +
                "down\n" +
                "due\n" +
                "during\n" +
                "each\n" +
                "eg\n" +
                "eight\n" +
                "either\n" +
                "eleven\n" +
                "else\n" +
                "elsewhere\n" +
                "empty\n" +
                "enough\n" +
                "etc\n" +
                "even\n" +
                "ever\n" +
                "every\n" +
                "everyone\n" +
                "everything\n" +
                "everywhere\n" +
                "except\n" +
                "few\n" +
                "fifteen\n" +
                "fify\n" +
                "fill\n" +
                "find\n" +
                "fire\n" +
                "first\n" +
                "five\n" +
                "for\n" +
                "former\n" +
                "formerly\n" +
                "forty\n" +
                "found\n" +
                "four\n" +
                "from\n" +
                "front\n" +
                "full\n" +
                "further\n" +
                "get\n" +
                "give\n" +
                "go\n" +
                "had\n" +
                "has\n" +
                "hasnt\n" +
                "have\n" +
                "he\n" +
                "hence\n" +
                "her\n" +
                "here\n" +
                "hereafter\n" +
                "hereby\n" +
                "herein\n" +
                "hereupon\n" +
                "hers\n" +
                "herself\n" +
                "him\n" +
                "himself\n" +
                "his\n" +
                "how\n" +
                "however\n" +
                "hundred\n" +
                "i\n" +
                "ie\n" +
                "if\n" +
                "in\n" +
                "inc\n" +
                "indeed\n" +
                "interest\n" +
                "into\n" +
                "is\n" +
                "it\n" +
                "its\n" +
                "itself\n" +
                "keep\n" +
                "last\n" +
                "latter\n" +
                "latterly\n" +
                "least\n" +
                "less\n" +
                "ltd\n" +
                "made\n" +
                "many\n" +
                "may\n" +
                "me\n" +
                "meanwhile\n" +
                "might\n" +
                "mill\n" +
                "mine\n" +
                "more\n" +
                "moreover\n" +
                "most\n" +
                "mostly\n" +
                "move\n" +
                "much\n" +
                "must\n" +
                "my\n" +
                "myself\n" +
                "name\n" +
                "namely\n" +
                "neither\n" +
                "never\n" +
                "nevertheless\n" +
                "next\n" +
                "nine\n" +
                "no\n" +
                "nobody\n" +
                "none\n" +
                "noone\n" +
                "nor\n" +
                "not\n" +
                "nothing\n" +
                "now\n" +
                "nowhere\n" +
                "of\n" +
                "off\n" +
                "often\n" +
                "on\n" +
                "once\n" +
                "one\n" +
                "only\n" +
                "onto\n" +
                "or\n" +
                "other\n" +
                "others\n" +
                "otherwise\n" +
                "our\n" +
                "ours\n" +
                "ourselves\n" +
                "out\n" +
                "over\n" +
                "own\n" +
                "part\n" +
                "per\n" +
                "perhaps\n" +
                "please\n" +
                "put\n" +
                "rather\n" +
                "re\n" +
                "same\n" +
                "see\n" +
                "seem\n" +
                "seemed\n" +
                "seeming\n" +
                "seems\n" +
                "serious\n" +
                "several\n" +
                "she\n" +
                "should\n" +
                "show\n" +
                "side\n" +
                "since\n" +
                "sincere\n" +
                "six\n" +
                "sixty\n" +
                "so\n" +
                "some\n" +
                "somehow\n" +
                "someone\n" +
                "something\n" +
                "sometime\n" +
                "sometimes\n" +
                "somewhere\n" +
                "still\n" +
                "such\n" +
                "system\n" +
                "take\n" +
                "ten\n" +
                "than\n" +
                "that\n" +
                "the\n" +
                "their\n" +
                "them\n" +
                "themselves\n" +
                "then\n" +
                "thence\n" +
                "there\n" +
                "thereafter\n" +
                "thereby\n" +
                "therefore\n" +
                "therein\n" +
                "thereupon\n" +
                "these\n" +
                "they\n" +
                "thick\n" +
                "thin\n" +
                "third\n" +
                "this\n" +
                "those\n" +
                "though\n" +
                "three\n" +
                "through\n" +
                "throughout\n" +
                "thru\n" +
                "thus\n" +
                "to\n" +
                "together\n" +
                "too\n" +
                "top\n" +
                "toward\n" +
                "towards\n" +
                "twelve\n" +
                "twenty\n" +
                "two\n" +
                "un\n" +
                "under\n" +
                "until\n" +
                "up\n" +
                "upon\n" +
                "us\n" +
                "very\n" +
                "via\n" +
                "was\n" +
                "we\n" +
                "well\n" +
                "were\n" +
                "what\n" +
                "whatever\n" +
                "when\n" +
                "whence\n" +
                "whenever\n" +
                "where\n" +
                "whereafter\n" +
                "whereas\n" +
                "whereby\n" +
                "wherein\n" +
                "whereupon\n" +
                "wherever\n" +
                "whether\n" +
                "which\n" +
                "while\n" +
                "whither\n" +
                "who\n" +
                "whoever\n" +
                "whole\n" +
                "whom\n" +
                "whose\n" +
                "why\n" +
                "will\n" +
                "with\n" +
                "within\n" +
                "without\n" +
                "would\n" +
                "yet\n" +
                "you\n" +
                "your\n" +
                "yours\n" +
                "yourself\n" +
                "yourselves";
        List<String> stopList = new ArrayList<>(Arrays.asList(stopWords.split("\n")));
        for(String word : wordsList){
            if(stopList.contains(word)){
                return true;
            }
        }

        return false;
    }

}
