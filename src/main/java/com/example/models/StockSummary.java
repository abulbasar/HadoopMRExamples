package com.example.models;

import lombok.Data;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.ObjectWriter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;


@Data
public class StockSummary implements Writable{

    private static ObjectReader objectReader = null;
    private static ObjectWriter objectWriter = null;

    private String symbol;
    private Integer year;
    private Double avgVolume;
    private Double maxOpen;
    private Double minOpen;

    static {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectWriter = objectMapper.writer();
        objectReader = objectMapper.reader(StockSummary.class);
    }

    public void write(DataOutput out) throws IOException {
        final String valueAsString = objectWriter.writeValueAsString(this);
        out.writeChars(valueAsString);
    }

    public void readFields(DataInput in) throws IOException {
        final String valueAsString = Text.readString(in);
        final StockSummary stockSummary = objectReader.readValue(valueAsString);
        try {
            BeanUtils.copyProperties(stockSummary, this);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }

}
