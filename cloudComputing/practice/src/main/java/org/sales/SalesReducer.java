package org.sales;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int totalQuantity = 0;
        for (IntWritable value : values) {
            totalQuantity += value.get();
        }
        context.write(key, new IntWritable(totalQuantity));
    }
}
