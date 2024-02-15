package org.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCount <inputPath> <outputPath>");
            System.exit(-1);
        }

        // Create a new Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        // Set the Jar by class
        job.setJarByClass(WordCount.class);

        // Set the Mapper and Reducer classes
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);

        // Set the output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
