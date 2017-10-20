package ru.mephi.lab1;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;


/**
 * Драйвер.
 */
public class Driver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Two params are required- <input path> <output path>");
        }

        getConf().set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(getConf());

        job.setJobName("Browser statistics");
        job.setJarByClass(Driver.class);


        //job.setInputFormatClass(BSInputFormat.class);
        //BSInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(BSMapper.class);
        job.setReducerClass(BSReducer.class);

        boolean success = job.waitForCompletion(true);
        Counter counter = job.getCounters().findCounter(BS_COUNTER.MALFORMED_ROWS);
        System.out.println("Malformed rows count:" + counter.getValue());
        return  success ? 1 : 0;
    }

}