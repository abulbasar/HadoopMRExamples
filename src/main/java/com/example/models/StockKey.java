package com.example.models;

import lombok.Data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class StockKey implements WritableComparable {

    private int year;
    private String symbol;

    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeChars(symbol);
    }

    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.symbol = Text.readString(in);
    }

    public int compareTo(Object other) {
        StockKey otherKey = (StockKey) other;
        if(this.year != otherKey.year) {
            return this.year - otherKey.year;
        }
        return this.symbol.compareTo(otherKey.symbol);
    }
}
