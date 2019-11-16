package com.example;

import com.example.models.StockKey;
import com.example.models.StockSummary;
import com.example.models.StockValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
$ hadoop jar HadoopMRExamples-1.0-SNAPSHOT.jar com.example.StockPriceCalculator /data/stocks.small.csv /data/output


*/
public class StockPriceCalculatorV2 {


    public static class StockMapper
            extends Mapper<Object, Text, StockKey, StockValue> {

        private final static DoubleWritable volume = new DoubleWritable(1);
        private Text symbol = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            final String[] tokens = value.toString().split(",");
            if (tokens.length > 0 && value.toString().startsWith("2016")) {
                final String date = tokens[0];
                final double open = Double.valueOf(tokens[1]);
                final double high = Double.valueOf(tokens[2]);
                final double low = Double.valueOf(tokens[3]);
                final double close = Double.valueOf(tokens[4]);
                final double volume = Double.valueOf(tokens[5]);
                //final double adjClose = Double.valueOf(tokens[6]);
                final String symbol = tokens[7];



                final StockKey stockKey = new StockKey();
                stockKey.setYear(Integer.valueOf(date.split("[-]")[0]));
                stockKey.setSymbol(symbol);

                final StockValue stockValue = new StockValue();
                stockValue.setOpen(open);
                stockValue.setClose(close);
                stockValue.setHigh(high);
                stockValue.setLow(low);
                stockValue.setVolume(volume);

                context.write(stockKey, stockValue);
            }
        }
    }


    public static class StockReducer
            extends Reducer<StockKey, StockValue, NullWritable, StockSummary>{

        public void reduce(StockKey key, Iterable<StockValue> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sumVolume = 0;
            double maxOpen = Double.NEGATIVE_INFINITY;
            double minOpen = Double.MAX_VALUE;
            int count = 0;
            for (StockValue value : values){
                ++count;
                sumVolume += value.getVolume();
                maxOpen = Math.max(maxOpen, value.getVolume());
                minOpen = Math.min(minOpen, value.getOpen());
            }
            final StockSummary stockSummary = new StockSummary();
            stockSummary.setYear(key.getYear());
            stockSummary.setSymbol(key.getSymbol());
            stockSummary.setAvgVolume(sumVolume/count);
            stockSummary.setMaxOpen(maxOpen);
            stockSummary.setMinOpen(minOpen);

            context.write(NullWritable.get(), stockSummary);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: " + StockPriceCalculatorV2.class.getName() + " <input> <output>");
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

        job.setJarByClass(StockPriceCalculatorV2.class);
        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(StockSummary.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
