package com.example.stock.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@AllArgsConstructor
public class StockValue implements Writable {

    private DoubleWritable volume;
    private DoubleWritable high;
    private DoubleWritable low;
    private DoubleWritable open;
    private DoubleWritable close;


    public StockValue(){
        volume = new DoubleWritable(0);
        high = new DoubleWritable(0);
        low = new DoubleWritable(0);
        open = new DoubleWritable(0);
        close = new DoubleWritable(0);
    }


    public void write(DataOutput out) throws IOException {
        volume.write(out);
        high.write(out);
        low.write(out);
        open.write(out);
        close.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        volume.readFields(in);
        high.readFields(in);
        low.readFields(in);
        open.readFields(in);
        close.readFields(in);
    }



}
