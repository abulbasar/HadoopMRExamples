package com.example.stock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/*

Problem statement: find average volume of trading per stock symbol.

Build jar
$ mvn package

$ sudo -u hdfs hadoop fs -mkdir -p /data
$ sudo -u hdfs hadoop fs -chmod 777 /data
$ wget https://raw.githubusercontent.com/abulbasar/data/master/stocks.small.csv
$ sudo -u hdfs hadoop fs -mkdir /data/stocks
$ hadoop fs -put stocks.small.csv /data/stocks
$ hadoop jar HadoopMRExamples-1.0-SNAPSHOT.jar com.example.stock.StockPriceCalculatorV1 /data/stocks.small.csv /data/output


*/
public class StockPriceCalculatorV1 extends Configured implements Tool {

    public static class StockMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private final static DoubleWritable volume = new DoubleWritable(1);
        private Text symbol = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
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

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: " + StockPriceCalculatorV1.class.getName() + " <input> <output>");
            System.exit(0);
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
        final Job job = Job.getInstance(conf, "Stock price calculator");

        job.setJarByClass(StockPriceCalculatorV1.class);
        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        //FileOutputFormat.setCompressOutput(job,true);
        //FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitFlag = ToolRunner.run(new StockPriceCalculatorV1(), args);
        System.exit(exitFlag);
    }
}
