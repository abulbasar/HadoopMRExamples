package com.example.stock.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ColumnTextInputFormat extends FileInputFormat<Text, Text> {



    public static class ColumnTextRecordReader extends RecordReader<Text, Text>{
        private Text key = null;
        private Text value = null;

        private long start;
        private long end;
        private long pos;
        private FSDataInputStream dataInputStream;


        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) inputSplit;
            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            pos = start;

            Configuration configuration = taskAttemptContext.getConfiguration();
            FileSystem fileSystem = FileSystem.get(configuration);
            dataInputStream = fileSystem.open(fileSplit.getPath());
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if(pos>=end){
                return false;
            }else{
                int keyWidth = 7;
                int lastNameWidth = 25;
                int firstNameWidth = 10;
                int dateWidth = 8;

                byte[] keyBytes = new byte[keyWidth];
                byte[] dateBytes = new byte[dateWidth];
                byte[] lastNameBytes = new byte[lastNameWidth];
                byte[] firstNameBytes = new byte[firstNameWidth];

                dataInputStream.readFully(pos, keyBytes);
                pos += keyWidth;
                dataInputStream.readFully(pos, lastNameBytes);
                pos += lastNameWidth;
                dataInputStream.readFully(pos, firstNameBytes);
                pos += firstNameWidth;
                dataInputStream.readFully(pos, dateBytes);
                pos += dateWidth;

                key = new Text(keyBytes);
                value = new Text(String.format("%s\t%s\t%s",
                        new String(lastNameBytes).trim(),
                        new String(firstNameBytes).trim(),
                        new String(dateBytes).trim()
                        ));

                return true;
            }
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            if(start == end){
                return 0f;
            }else{
                return Math.min(1f, 1f * (pos-start)/(end-start));
            }
        }

        @Override
        public void close() throws IOException {
            dataInputStream.close();

        }
    }

    private long recordWidth;

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit
            , TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {

        final RecordReader<Text, Text> recordReader = new ColumnTextRecordReader();
        recordReader.initialize(inputSplit, taskAttemptContext);
        return recordReader;
    }

    @Override
    protected long computeSplitSize(long blockSize, long minSize, long maxSize){
        long defaultSize = super.computeSplitSize(blockSize, minSize, maxSize);
        if(defaultSize<recordWidth){
            return recordWidth;
        }

        long splitSize = ((long)Math.floor((double)defaultSize/recordWidth)) * recordWidth;

        return splitSize;
    }
}
