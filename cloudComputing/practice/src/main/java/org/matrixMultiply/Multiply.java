package org.matrixMultiply;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;

    Pair() {}
    Pair(int i, int j) {
        this.i = i;
        this.j = j;
    }

    // Serialization
    public void write(DataOutput out) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
    }

    // Deserialization
    public void readFields(DataInput in) throws IOException {
        i = in.readInt();
        j = in.readInt();
    }

    // Comparison for sorting
    public int compareTo(Pair p) {
        if (i == p.i) {
            return Integer.compare(j, p.j);
        }
        return Integer.compare(i, p.i);
    }

    // Equals method
    public boolean equals(Object o) {
        if (!(o instanceof Pair))
            return false;
        Pair p = (Pair) o;
        return i == p.i && j == p.j;
    }

    // Hash code for partitioning
    public int hashCode() {
        return Objects.hash(i, j);
    }

    // ToString method
    public String toString() {
        return "(" + i + "," + j + ")";
    }
}

public class Multiply {

    public static class MultiplyMapper extends Mapper<LongWritable, Text, Pair, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length == 3) {
                int row = Integer.parseInt(tokens[0]);
                int col = Integer.parseInt(tokens[1]);
                String matrix = context.getInputSplit().toString().contains("input1") ? "A" : "B";
                context.write(new Pair(row, col), new Text(matrix + "," + tokens[2]));
            }
        }
    }

    public static class MultiplyReducer extends Reducer<Pair, Text, Pair, DoubleWritable> {
        @Override
        public void reduce(Pair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Integer, Double> matrixA = new HashMap<>();
            Map<Integer, Double> matrixB = new HashMap<>();

            // Iterate over all values and populate matrixA and matrixB
            for (Text value : values) {
                String[] parts = value.toString().split(",");
                String matrixLabel = parts[0];
                double elementValue = Double.parseDouble(parts[1]);

                // Determine which matrix the value belongs to
                if (matrixLabel.equals("A")) {
                    matrixA.put(key.i, elementValue); // Corrected key usage
                } else if (matrixLabel.equals("B")) {
                    matrixB.put(key.j, elementValue); // Corrected key usage
                }
            }

            // Debugging output to check the content of matrixA and matrixB
            System.out.println("Matrix A for key " + key + ": " + matrixA);
            System.out.println("Matrix B for key " + key + ": " + matrixB);

            // Calculate the result for this cell of the output matrix
            double result = 0.0;
            for (int k = 0; k < Math.max(matrixA.size(), matrixB.size()); k++) {
                // Multiply corresponding elements and sum up
                result += matrixA.getOrDefault(k, 0.0) * matrixB.getOrDefault(k, 0.0);
            }

            context.write(key, new DoubleWritable(result));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Matrix Multiplication");

        job.setJarByClass(Multiply.class);
        job.setMapperClass(MultiplyMapper.class);
        job.setReducerClass(MultiplyReducer.class);

        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapOutputKeyClass(Pair.class);
        job.setMapOutputValueClass(Text.class);

        // Set the input format for the first input file (Matrix A)
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MultiplyMapper.class);
        // Set the input format for the second input file (Matrix B)
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MultiplyMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
