package org.transactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.sales.SalesMapper;
import org.sales.SalesReducer;

public class TotalSalesByCategory {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TotalSalesByCategory <inputPath> <outputPath>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "total sales by category");

        job.setJarByClass(TotalSalesByCategory.class);
        job.setMapperClass(SaleNewMapper.class);
        job.setReducerClass(SaleNewReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
