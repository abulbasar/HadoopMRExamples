package com.example.models;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class StockValue implements Writable {

    private double volume;
    private double high;
    private double low;
    private double open;
    private double close;


    public void write(DataOutput out) throws IOException {
        out.writeDouble(volume);
        out.writeDouble(high);
        out.writeDouble(low);
        out.writeDouble(open);
        out.writeDouble(close);
    }

    public void readFields(DataInput in) throws IOException {
        volume = in.readDouble();
        high = in.readDouble();
        low = in.readDouble();
        open = in.readDouble();
        close = in.readDouble();
    }



}
