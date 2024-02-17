package org.matrixMultiply;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

// Custom WritableComparable to represent a pair of integers
class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;

    Pair() {}

    Pair(int i, int j) {
        this.i = i;
        this.j = j;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        i = in.readInt();
        j = in.readInt();
    }

    @Override
    public int compareTo(Pair o) {
        if (i != o.i) {
            return Integer.compare(i, o.i);
        } else {
            return Integer.compare(j, o.j);
        }
    }

    @Override
    public String toString() {
        return i + ","+ j;
    }

    public int getRow() {
        return i;
    }

    public int getCol() {
        return j;
    }
}

public class Multiply {

    public static class Matrix1Mapper extends Mapper<LongWritable, Text, Pair, Text> {
        Pair outputKey = new Pair();
        Text outputValue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int columns = Integer.parseInt(conf.get("p"));
            // Parse input line
            String[] tokens = value.toString().split(",");
            int row = Integer.parseInt(tokens[0]);
            int col = Integer.parseInt(tokens[1]);
            double element = Double.parseDouble(tokens[2]);

            for (int k = 0; k < columns; k++) {
                outputKey.i = row;
                outputKey.j = k;
                outputValue.set("A" + "," + col + "," + element);
                context.write(outputKey,outputValue);
            }

        }
    }

    public static class Matrix2Mapper extends Mapper<LongWritable, Text, Pair, Text> {
        Pair outputKey = new Pair();
        Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int rows = Integer.parseInt(conf.get("m"));
            String[] tokens = value.toString().split(",");
            int row = Integer.parseInt(tokens[0]);
            int col = Integer.parseInt(tokens[1]);
            double element = Double.parseDouble(tokens[2]);

            for (int i = 0; i < rows; i++) {
                outputKey.i = i;
                outputKey.j = col;
                outputValue.set("B" + "," + row + "," + element);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class MatrixReducer extends Reducer<Pair, Text, Text, Text> {
        @Override
        protected void reduce(Pair key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String[] tokens;

            HashMap<Integer, Double> matrixA = new HashMap<>();
            HashMap<Integer, Double> matrixB = new HashMap<>();

            for(Text value: values){
                tokens = value.toString().split(",");
                if(tokens[0].equals("A")){
                    matrixA.put(Integer.parseInt(tokens[1]), Double.parseDouble(tokens[2]));
                }else{
                    matrixB.put(Integer.parseInt(tokens[1]), Double.parseDouble(tokens[2]));
                }
            }

            int n = Integer.parseInt(context.getConfiguration().get("n"));
            double result = 0.0;
            double matrixACellValue;
            double matrixBCellValue;
            for (int i = 0; i < n; i++) {
                matrixACellValue = matrixA.containsKey(i) ? matrixA.get(i) : 0.0;
                matrixBCellValue = matrixB.containsKey(i) ? matrixB.get(i) : 0.0;
                result += matrixACellValue * matrixBCellValue;
            }
            if(result != 0.0){
                context.write(new Text(key.toString()), new Text(Double.toString(result)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: org.matrixMultiply.Multiply <inputMatrixA> <inputMatrixB> <outputPath>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("m", "1000");
        conf.set("n", "1000");
        conf.set("p", "1000");

        Job job = Job.getInstance(conf, "Matrix Multiply");

        job.setJarByClass(Multiply.class);
        job.setMapperClass(Matrix1Mapper.class);
        job.setMapperClass(Matrix2Mapper.class);
        job.setReducerClass(MatrixReducer.class);
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Matrix1Mapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Matrix2Mapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
