import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Runner
{
	private static Job createJob(String jobName, String input, String output, boolean useCombiner) throws IOException
	{
		Job job = new Job();
		job.setJobName(jobName);
		job.setJarByClass(Runner.class);

		job.setMapperClass(DistinctUserMapper.class);
		job.setReducerClass(DistinctUserReducer.class);
		
		if (useCombiner)
			job.setCombinerClass(DistinctUserReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job;
	}
	
	public static void main(String[] args) throws Exception
	{
		// For debugging.
		// if (args.length == 0)
		//	args = new String[] { "input", "output", "true" };
		
		boolean useCombiner = (args.length > 2 && args[2].equalsIgnoreCase("withCombiner"));

		Job job = createJob("Runner", args[0], args[1], useCombiner);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

