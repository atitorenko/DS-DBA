import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class MapperNLine extends Mapper<LongWritable, Text, Text, LongWritable>{
	private final static IntWritable one = new IntWritable(1);
	private Text browserName = new Text();
	Pattern pattern = Pattern.compile(".*(\"([a-zA-Z0-9\\.\\/]*).*\")$");
    
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	Matcher m;
    	
    	StringTokenizer itr = new StringTokenizer(value.toString());
//	    while (itr.hasMoreTokens()) {
//	    	word.set(itr.nextToken());
//	        context.write(word, one);
//	        }
	    }
    private String getBrowserName(String line) {
    	
    }
}
}
