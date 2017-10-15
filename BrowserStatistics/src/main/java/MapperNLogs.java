package main.java;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperNLogs extends Mapper<LongWritable, Text, Text, LongWritable>{
    private final static LongWritable one = new LongWritable(1);
    private Text browserName = new Text();
    private Pattern pattern = Pattern.compile(".*(\"([a-zA-Z0-9\\.\\/]*).*\")$");
    private Matcher m;
    private Text browser = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] lines = value.toString().split("(\\r\\n|\\r|\\n)");
        while (end != lines.length() - 1) {
            end = lines.indexOf('\n', begin);
            m = pattern.matcher(lines.substring(begin, end - 1));
            if (m.find()) {
                begin = end+1;
                browser.set(m.group(2));
                context.write(browser, one);
            }
        }
    }
}
