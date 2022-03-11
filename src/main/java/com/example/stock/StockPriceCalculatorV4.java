package com.example.stock;

import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/*

Problem statement: Find top 10 stocks that have given the highest return in 2015

Build jar
$ mvn package

$ sudo -u hdfs hadoop fs -mkdir -p /data
$ sudo -u hdfs hadoop fs -chmod 777 /data
$ wget https://raw.githubusercontent.com/abulbasar/data/master/stocks.small.csv
$ sudo -u hdfs hadoop fs -mkdir /data/stocks
$ hadoop fs -put stocks.small.csv /data/stocks
$ hadoop jar HadoopMRExamples-1.0-SNAPSHOT.jar \
com.example.stock.StockPriceCalculatorV1 /data/stocks/ /data/output


*/
public class StockPriceCalculatorV4 extends Configured implements Tool {

    public static class StockMapper extends Mapper<Object, Text, Text, Text> {

        private final Text valueTuple = new Text();
        private final Text keyTuple = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            final String s = value.toString();

            if (s.startsWith("2015")) {
                String[] tokens = s.split(",");
                final Double price = Double.valueOf(tokens[6]); // 6th field is adjClose field.
                final String symbol = tokens[7];
                final String date = tokens[0];
                this.keyTuple.set(symbol);
                this.valueTuple.set(String.format("%s,%f", date, price) );
                context.write(this.keyTuple, this.valueTuple);
            }
        }
    }

    @Data
    public static class Stock implements Serializable{
        private String date;
        private Double price;
    }


    public static class StockReducer
            extends Reducer<Text, Text, Text, DoubleWritable> {

        private DoubleWritable valueTuple = new DoubleWritable();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            final List<Stock> records = new ArrayList<>();
            for (Text val : values) {
                final String s = val.toString();
                final String[] tokens = s.split(",");
                final String date = tokens[0];
                final Double price = Double.valueOf(tokens[1]);
                final Stock stock = new Stock();
                stock.setDate(date);
                stock.setPrice(price);
                records.add(stock);
            }
            records.sort(Comparator.comparing(Stock::getDate));
            final Stock firstTuple = records.get(0);
            final Stock lastTuple = records.get(records.size() - 1);
            final Double firstPrice = firstTuple.getPrice();
            final Double lastPrice = lastTuple.getPrice();
            final Double yearlyReturn = (lastPrice-firstPrice)/firstPrice;
            valueTuple.set(yearlyReturn);
            context.write(key, valueTuple);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: " + StockPriceCalculatorV4.class.getName() + " <input> <output>");
            System.exit(0);
        }
        final Path inputPath = new Path(args[0]);
        final Path outputPath = new Path(args[1]);

        /*
         * Delete the output directory if it exists.
         */
        final Configuration conf = new Configuration();
        final FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        conf.set("hadoop.tmp.dir", "/tmp/hadoop");
        conf.set("mapred.textoutputformat.separator", ",");

        // Specify record delimiter. Default record delimiter = \n
        //conf.set("textinputformat.record.delimiter",",");

        final Job job = Job.getInstance(conf, getClass().getName());

        job.setJarByClass(StockPriceCalculatorV4.class);
        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        /*

        Compress output files of map reduce job and specify the codecs.

        */
        FileOutputFormat.setCompressOutput(job,true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        job.setSpeculativeExecution(false);

        int success = job.waitForCompletion(true) ? 0 : 1;

        // Copy the file from HDFS to local file system
        //Path resultsFile = new Path(outputPath + "/part-r-00000");
        //fileSystem.copyToLocalFile(resultsFile, new Path("/tmp/stats.txt"));

        return success;
    }

    public static void main(String[] args) throws Exception {
        int exitFlag = ToolRunner.run(new StockPriceCalculatorV4(), args);
        System.exit(exitFlag);
    }
}
