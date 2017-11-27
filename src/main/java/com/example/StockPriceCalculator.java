package com.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
Sample data file: https://github.com/abulbasar/data/blob/master/stocks.small.csv
Problem statement: find average volume of trading per stock symbol.

*/
public class StockPriceCalculator {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, DoubleWritable>{

    private final static DoubleWritable volume = new DoubleWritable(1);
    private Text symbol = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");
      if(tokens.length > 0  && value.toString().startsWith("2016")){
	      volume.set(Double.valueOf(tokens[5]));
	      symbol.set(tokens[7]);
	
	      context.write(symbol, volume);
      }
    }
  }


  public static class AvgReducer
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
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

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Stock price calculator");
    job.setJarByClass(StockPriceCalculator.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(AvgReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
