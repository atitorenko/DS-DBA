import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public class Driver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		if (args.length != 2) {
			System.out.println("Two params are required- <input path> <output path>");
		}
		Job job = Job.getInstance(getConf());
		job.setJobName("Browser statistics");
		job.setJarBy
		
		return 0;
	}
	
}