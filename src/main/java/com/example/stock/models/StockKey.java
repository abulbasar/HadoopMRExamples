package com.example.stock.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@AllArgsConstructor
public class StockKey  implements WritableComparable<StockKey>{

    private IntWritable year;
    private Text symbol;

    public StockKey(){
        year = new IntWritable();
        symbol = new Text();
    }

    public void write(DataOutput out) throws IOException {
        year.write(out);
        symbol.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        year.readFields(in);
        symbol.readFields(in);
    }

    public int compareTo(StockKey other) {
        if(this.year.get() - other.getYear().get() == 0) {
            return this.symbol.toString().compareTo(other.symbol.toString());
        }
        return 0;
    }
}
