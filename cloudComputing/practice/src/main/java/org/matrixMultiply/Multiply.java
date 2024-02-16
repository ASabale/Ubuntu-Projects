package org.matrixMultiply;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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

    public int getRow(){
        return i;
    }

    public int getCol(){
        return j;
    }
}

class MatrixCell implements Writable {
    public char label;
    public double value;

    MatrixCell() {
    }

    MatrixCell(char label, double value) {
        this.label = label;
        this.value = value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChar(label);
        out.writeDouble(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        label = in.readChar();
        value = in.readDouble();
    }

    @Override
    public String toString() {
        return label + ":" + value;
    }
}


public class Multiply {

    public static class MatrixAMapper extends Mapper<LongWritable, Text, Pair, MatrixCell> {
        private Pair outputKey = new Pair();
        private MatrixCell outputValue = new MatrixCell();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            int row = Integer.parseInt(tokens[0]);
            int col = Integer.parseInt(tokens[1]);
            double matrixValue = Double.parseDouble(tokens[2]);

            outputKey.i = row;
            outputKey.j = col;
            outputValue.label = 'A';
            outputValue.value = matrixValue;

            context.write(outputKey, outputValue);
        }
    }

    public static class MatrixBMapper extends Mapper<LongWritable, Text, Pair, MatrixCell> {
        private Pair outputKey = new Pair();
        private MatrixCell outputValue = new MatrixCell();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            int row = Integer.parseInt(tokens[0]);
            int col = Integer.parseInt(tokens[1]);
            double matrixValue = Double.parseDouble(tokens[2]);

            outputKey.i = row;
            outputKey.j = col;
            outputValue.label = 'B';
            outputValue.value = matrixValue;

            context.write(outputKey, outputValue);
        }
    }

    public static class MultiplyReducer extends Reducer<Pair, MatrixCell, Pair, DoubleWritable> {
        private Map<Pair, Double> matrixAValues = new HashMap<>();
        private Map<Pair, Double> matrixBValues = new HashMap<>();
        private DoubleWritable outputValue = new DoubleWritable();

        @Override
        protected void reduce(Pair key, Iterable<MatrixCell> values, Context context) throws IOException, InterruptedException {
            matrixAValues.clear();
            matrixBValues.clear();

            // Iterate through the values and populate the hashmaps
            for (MatrixCell cell : values) {
                Pair cellKey = new Pair(key.i, key.j);
                if (cell.label == 'A') {
                    matrixAValues.put(cellKey, cell.value);
                } else if (cell.label == 'B') {
                    matrixBValues.put(cellKey, cell.value);
                }
            }

            //find matrix dimension dynamically
            int numRowsA = matrixAValues.keySet().stream().mapToInt(Pair::getRow).max().orElse(0) + 1;
            int numColsA = matrixAValues.keySet().stream().mapToInt(Pair::getCol).max().orElse(0) + 1;
            int numRowsB = matrixBValues.keySet().stream().mapToInt(Pair::getRow).max().orElse(0) + 1;
            int numColsB = matrixBValues.keySet().stream().mapToInt(Pair::getCol).max().orElse(0) + 1;

            // Create 2D arrays for matrices A and B
            double[][] matrixA;
            double[][] matrixB;
            if(numColsA>numRowsB){
                matrixA = new double[numRowsA][numColsA];
                matrixB = new double[numColsA][numColsB];
            }
            else if(numColsA<numRowsB){
                matrixA = new double[numRowsA][numRowsB];
                matrixB = new double[numRowsB][numColsB];
            }else{
                matrixA = new double[numRowsA][numColsA];
                matrixB = new double[numRowsB][numColsB];
            }


            // Initialize arrays with zeros
            for (int i = 0; i < numRowsA; i++) {
                for (int j = 0; j < numColsA; j++) {
                    matrixA[i][j] = 0.0;
                }
            }

            for (int i = 0; i < numRowsB; i++) {
                for (int j = 0; j < numColsB; j++) {
                    matrixB[i][j] = 0.0;
                }
            }

            // Fill in the arrays using the values from the hashmaps
            for (Map.Entry<Pair, Double> entry : matrixAValues.entrySet()) {
                Pair aKey = entry.getKey();
                matrixA[aKey.getRow()][aKey.getCol()] = entry.getValue();
            }

            for (Map.Entry<Pair, Double> entry : matrixBValues.entrySet()) {
                Pair bKey = entry.getKey();
                matrixB[bKey.getRow()][bKey.getCol()] = entry.getValue();
            }

            // Perform matrix multiplication
            int rowsA = matrixA.length;
            int colsA = matrixA[0].length;
            int colsB = matrixB[0].length;
            double[][] result = new double[rowsA][colsB];

            for (int i = 0; i < rowsA; i++) {
                for (int j = 0; j < colsB; j++) {
                    for (int k = 0; k < colsA; k++) {
                        result[i][j] += matrixA[i][k] * matrixB[k][j];
                    }
                }
            }

            for (int i = 0; i < rowsA; i++) {
                for (int j = 0; j < colsB; j++) {
                    if (result[i][j]!=0.0){
                        outputValue.set(result[i][j]);
                        context.write(key, outputValue);
                    }
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MatrixMultiplication <inputMatrixA> <inputMatrixB> <outputPath>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Matrix Multiplication");

        job.setJarByClass(Multiply.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MatrixAMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MatrixBMapper.class);
        job.setReducerClass(MultiplyReducer.class);
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
