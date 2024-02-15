package org.sales;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text productID = new Text();
    private IntWritable quantity = new IntWritable();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        // Skip the first line (assuming it contains column headers)
        if (key.get() == 0 && line.contains("CustomerID,ProductID,Quantity")) {
            return;
        }
        String[] fields = value.toString().split(",");
        if (fields.length == 3) {
            productID.set(fields[1].trim());
            quantity.set(Integer.parseInt(fields[2].trim()));
            context.write(productID, quantity);
        }
    }
}
