package ru.mephi.lab1;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Кастомизированный InputFormat, вызывающий кастомизированный RecordReader.
 */
public class BSInputFormat extends TextInputFormat {
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new BSRecordReader();
    }
}
