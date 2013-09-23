import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SmartRunner
{
	private static Job createJob(String jobName, String input, String output) throws IOException
	{
		Configuration conf = new Configuration();
		conf.set(XmlInputFormat.START_TAG_KEY, "<row");
		conf.set(XmlInputFormat.END_TAG_KEY, "/>");

		Job job = new Job(conf, jobName);

		job.setJarByClass(SmartRunner.class);
		job.setMapperClass(SmartMapper.class);
		job.setCombinerClass(SmartCombiner.class);
		job.setReducerClass(SmartReducer.class);

		job.setMapOutputValueClass(SortedMapWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(MedianAndStdDev.class);
		
		job.setInputFormatClass(XmlInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job;
	}
	
	public static void main(String[] args) throws Exception
	{
		// For debugging.
		// if (args.length == 0)
		//	args = new String[] { "input", "output" };
		
		Job job = createJob("SmartRunner", args[0], args[1]);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
