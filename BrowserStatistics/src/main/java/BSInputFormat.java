package main.java;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class BSInputFormat extends TextInputFormat {
    @Override
    //TODO: нафига аргументы.
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new BSRecordReader();
    }
}
