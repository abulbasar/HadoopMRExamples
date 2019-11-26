package com.example.stock.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


@Data
@AllArgsConstructor
public class StockSummary implements Writable{


    private Text symbol;
    private IntWritable year;
    private DoubleWritable avgVolume;
    private DoubleWritable maxOpen;
    private DoubleWritable minOpen;

    public StockSummary(){
        symbol = new Text();
        year = new IntWritable();
        avgVolume = new DoubleWritable();
        maxOpen = new DoubleWritable();
        minOpen = new DoubleWritable();
    }

    public void write(DataOutput out) throws IOException {
        symbol.write(out);
        year.write(out);
        avgVolume.write(out);
        maxOpen.write(out);
        minOpen.write(out);
    }

    public void readFields(DataInput in) throws IOException {
         symbol.readFields(in);
         year.readFields(in);
         avgVolume.readFields(in);
         maxOpen.readFields(in);
         minOpen.readFields(in);
    }

    @Override
    public String toString(){
        return symbol.toString() + "," + year.get() + "," + avgVolume.get() + "," + maxOpen.get() + "," + minOpen.get();
    }

}
