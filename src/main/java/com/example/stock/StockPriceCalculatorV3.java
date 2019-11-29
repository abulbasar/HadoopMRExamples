package com.example.stock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/*

Problem statement: find average volume of trading per stock symbol and sort the result by avg volume.

*/
public class StockPriceCalculatorV3 extends Configured implements Tool {



    public static class StockMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private final static DoubleWritable volume = new DoubleWritable(1);
        private Text symbol = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length > 0 && value.toString().startsWith("2016")) {
                volume.set(Double.valueOf(tokens[5]));
                symbol.set(tokens[7]);

                context.write(symbol, volume);
            }
        }
    }


    public static class StockReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                ++count;
            }
            result.set(sum / count);
            context.write(key, result);
        }
    }

    public static class KeyValueMapper
            extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] tokens = value.toString().split("\t");
            if (tokens.length == 2) {
                double avgVolume = Double.valueOf(tokens[1]);
                context.write(new DoubleWritable(avgVolume), new Text(tokens[0]));
            }
        }
    }

    public static class IdentityReducer
            extends Reducer<DoubleWritable, Text, Text, DoubleWritable>{

        @Override
        public void reduce(DoubleWritable avgVolume, Iterable<Text> symbols, Context context)
                throws IOException, InterruptedException {

            for (Text symbol: symbols) {
                context.write(symbol, avgVolume);
            }
        }
    }



    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: " + StockPriceCalculatorV3.class.getName() + " <input> <output>");
            System.exit(0);
        }
        final Path inputPath = new Path(args[0]);
        final Path outputPath = new Path(args[1]);
        final Path intermediatePath = new Path(args[1] + "-intermediate");


        /*
         * Delete the output directory if it exists.
         */
        final Configuration conf = new Configuration();
        final FileSystem fileSystem = inputPath.getFileSystem(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        /*
         * Delete the intermediate directory if it exists.
         */

        if (fileSystem.exists(intermediatePath)) {
            fileSystem.delete(intermediatePath, true);
        }

        final Job job1 = Job.getInstance(conf, "Stock price calculator");

        job1.setJarByClass(StockPriceCalculatorV3.class);
        job1.setMapperClass(StockMapper.class);
        job1.setReducerClass(StockReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, intermediatePath);


        int success = job1.waitForCompletion(true) ? 0 : 1;

        if(success != 0){
            return success;
        }

        final Job job2 = Job.getInstance(conf, getClass().getName());

        job2.setJarByClass(StockPriceCalculatorV3.class);
        job2.setMapperClass(KeyValueMapper.class);
        job2.setReducerClass(IdentityReducer.class);

        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job2, intermediatePath);
        FileOutputFormat.setOutputPath(job2, outputPath);

        success = job2.waitForCompletion(true) ? 0 : 1;

        // Delete intermediate directory
        if (fileSystem.exists(intermediatePath)) {
            fileSystem.delete(intermediatePath, true);
        }

        return success;
    }

    public static void main(String[] args) throws Exception {
        int exitFlag = ToolRunner.run(new StockPriceCalculatorV3(), args);
        System.exit(exitFlag);
    }
}
