package com.example.stock;

import com.example.stock.models.StockKey;
import com.example.stock.models.StockSummary;
import com.example.stock.models.StockValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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

/*

Problem statement: find average volume, min open and max open price for year stock symbol and year.
Here we need two group by fields and 3 aggregated fields.

*/


public class StockPriceCalculatorV2 extends Configured implements Tool{

    private static void println(Object ... args){
        for(Object s: args){
            System.out.print(s);
        }
        System.out.println();
    }

    public static class StockMapper
            extends Mapper<LongWritable, Text, StockKey, StockValue> {

        @Override
        protected void setup(Context context){
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            if(value.toString().startsWith("2016")) {
                final String line = value.toString();
                String[] tokens = line.split(",");
                if(tokens.length>0) {
                    final String date = tokens[0];
                    final double open = Double.valueOf(tokens[1]);
                    final double high = Double.valueOf(tokens[2]);
                    final double low = Double.valueOf(tokens[3]);
                    final double close = Double.valueOf(tokens[4]);
                    final double volume = Double.valueOf(tokens[5]);
                    final String symbol = tokens[7];

                    int year = Integer.parseInt(date.split("[-]")[0]);

                    final StockKey stockKey = new StockKey(
                            new IntWritable(year),
                            new Text(symbol)
                    );

                    final StockValue stockValue = new StockValue(
                            new DoubleWritable(volume),
                            new DoubleWritable(high),
                            new DoubleWritable(low),
                            new DoubleWritable(open),
                            new DoubleWritable(close)
                    );

                    context.write(stockKey, stockValue);
                }
            }
        }
    }


    public static class StockReducer
            extends Reducer<StockKey, StockValue, NullWritable, StockSummary>{

        StockSummary stockSummary;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }

        @Override
        public void reduce(StockKey key, Iterable<StockValue> values,Context context)
                throws IOException, InterruptedException {

            double sumVolume = 0;
            double maxOpen = Double.NEGATIVE_INFINITY;
            double minOpen = Double.MAX_VALUE;
            int count = 0;
            for (StockValue value : values){
                ++count;
                double volume = value.getVolume().get();
                double open = value.getOpen().get();
                sumVolume += volume;
                maxOpen = Math.max(maxOpen, open);
                minOpen = Math.min(minOpen, open);
            }

            double avgVolume = sumVolume/count;
            stockSummary = new StockSummary(key.getSymbol()
                    , key.getYear()
                    , new DoubleWritable(avgVolume)
                    , new DoubleWritable(maxOpen)
                    , new DoubleWritable(minOpen)
            );

            context.write(NullWritable.get(), stockSummary);
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: " + StockPriceCalculatorV2.class.getName() + " <input> <output>");
            return -1;
        }
        final Path inputPath = new Path(args[0]);
        final Path outputPath = new Path(args[1]);

        /*
         * Delete the output directory if it exists.
         */
        final Configuration conf = new Configuration();
        final FileSystem fileSystem = inputPath.getFileSystem(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        /*
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.job.maps","1");
        conf.set("mapreduce.job.reduces","1");
        */


        final Job job = Job.getInstance(conf, "Stock price calculator");

        job.setJarByClass(StockPriceCalculatorV2.class);

        job.setMapperClass(StockPriceCalculatorV2.StockMapper.class);
        job.setReducerClass(StockPriceCalculatorV2.StockReducer.class);

        job.setMapOutputKeyClass(StockKey.class);
        job.setMapOutputValueClass(StockValue.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(StockSummary.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setSpeculativeExecution(false);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration()
                ,new StockPriceCalculatorV2(), args);
        System.exit(exitCode);
    }
}
