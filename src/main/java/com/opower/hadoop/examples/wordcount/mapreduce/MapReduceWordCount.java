package com.opower.hadoop.examples.wordcount.mapreduce;

import com.google.inject.Guice;
import com.opower.hadoop.examples.wordcount.WordCountGuiceModule;
import com.opower.hadoop.examples.wordcount.WordCountService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * A variation on the word count MapReduce job that uses Guice to dependency inject a {@link WordCountService} at runtime
 * in the reduce phase.
 */
public class MapReduceWordCount extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            MapReduceWordCountDAO wordCountDAO = new MapReduceWordCountDAO(key, values, context);

            WordCountService wordCountService =
                    Guice.createInjector(new WordCountGuiceModule(wordCountDAO))
                            .getInstance(WordCountService.class);

            wordCountService.countWord(key.toString());
        }
    }

    public int run(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];

        FileSystem fileSystem = FileSystem.get(getConf());
        fileSystem.delete(new Path(outputPath), true);

        Job job = new Job(getConf(), "wordcount");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
        return 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MapReduceWordCount(), args);
        System.exit(res);
    }
}
