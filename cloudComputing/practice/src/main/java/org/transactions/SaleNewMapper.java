package org.transactions;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SaleNewMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text category = new Text();
    private LongWritable salesAmount = new LongWritable();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        // Skip the first line (assuming it contains column headers)
        if (key.get() == 0 && line.contains("TransactionID,ProductID,Category,Quantity,Price")) {
            return;
        }

        String[] fields = line.split(",");
        if (fields.length == 5) {
            category.set(fields[2].trim());
            long quantity = Long.parseLong(fields[3].trim());
            long price = Long.parseLong(fields[4].trim());
            salesAmount.set(quantity * price);
            context.write(category, salesAmount);
        }
    }
}
