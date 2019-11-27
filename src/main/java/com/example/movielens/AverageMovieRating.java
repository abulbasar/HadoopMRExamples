package com.example.movielens;

import com.google.common.base.Splitter;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/*

Problem statement: find average per movie id. Project movieId, title, rating count and avg rating.

Input files: movies.csv, ratings.csv

*/
public class AverageMovieRating extends Configured implements Tool {

    private static CsvParser csvParser;

    public static class MovieMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text movieId = new Text();
        private Text title = new Text();

        @Override
        public void setup(Context context) throws IOException{
            InputSplit inputSplit = context.getInputSplit();
            System.out.println(inputSplit.toString());
            if(inputSplit.getLocationInfo()!=null) {
                for (SplitLocationInfo splitLocationInfo : inputSplit.getLocationInfo()) {
                    System.out.println(splitLocationInfo.getLocation());
                }
            }
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            final String line = value.toString();
            String[] tokens = csvParser.parseLine(line);
            if(tokens.length == 3 && !line.startsWith("movieId")){
                movieId.set(tokens[0]);
                title.set("movie::" + tokens[1]);
                context.write(movieId, title);
            }
        }
    }

    public static class RatingMapper
            extends Mapper<Object, Text, Text, Text> {


        private Text movieId = new Text();
        private Text rating = new Text();


        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            final String line = value.toString();
            final String[] tokens = csvParser.parseLine(line);
            if(tokens.length == 4 && !line.startsWith("userId")){
                movieId.set(tokens[1]);
                rating.set("rating::" + tokens[2]);
                context.write(movieId, rating);
            }
        }
    }


    public static class JoinReducer extends Reducer<Text, Text, NullWritable, Text> {

        private Text avgRatingOutput = new Text();
        private NullWritable nullWritable = NullWritable.get();

        @Override
        public void reduce(Text movieId, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String title = "";
            double ratingSum = 0.0;
            int ratingCount = 0;
            for(Text value: values) {
                String line = value.toString();
                if(line.startsWith("movie::")){
                    title = line.substring(7);
                }else if(line.startsWith("rating::")){
                    double rating = Double.valueOf(line.substring(8));
                    ratingSum += rating;
                    ++ratingCount;
                }
            }
            double avgRating = ratingSum/ratingCount;
            avgRatingOutput.set(String.format("%s,%s,%f,%d", movieId.toString(), title, avgRating, ratingCount));
            context.write(nullWritable, avgRatingOutput);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage: " + AverageMovieRating.class.getName() + " <input1 - movies.csv> <input2 - ratings.csv> <output>");
            System.exit(0);
        }
        final Path inputPath1 = new Path(args[0]);
        final Path inputPath2 = new Path(args[1]);
        final Path outputPath = new Path(args[2]);

        /*
         * Delete the output directory if it exists.
         */
        final Configuration conf = new Configuration();
        final FileSystem fileSystem = inputPath1.getFileSystem(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        final Job job = Job.getInstance(conf, "Movie ratings calculator");

        job.setJarByClass(AverageMovieRating.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, RatingMapper.class);

        FileOutputFormat.setOutputPath(job, outputPath);


        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        CsvParserSettings settings = new CsvParserSettings();
        AverageMovieRating.csvParser = new CsvParser(settings);

        int exitFlag = ToolRunner.run(new AverageMovieRating(), args);
        System.exit(exitFlag);
    }
}
