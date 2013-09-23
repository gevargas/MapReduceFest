import java.io.File;
import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HotwordsRunner
{
	private static final String BLOOM_FILTER_FILE = "hotwords.bloom"; 

	public static void main(String[] args) throws Exception
	{
		checkParameters(args);
		String hotwordsFile = args[0];
		String inputDir = args[1];
		String outputDir = args[2];
		
		setup(hotwordsFile, outputDir);
		
		Job job = createJob("Runner", inputDir, outputDir);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	private static void checkParameters(String[] args)
	{
		if (args.length < 3)
		{
			System.err.println("Usage: HotwordsRunner <hotwordsFile> <inputDir> <outputDir>");
			System.exit(1);
		} 		
	}
	
	private static void setup(String hotwordsFile, String outputDir) throws Exception
	{
		// Train the Bloom Filter from the hot-words list.
		// Actually, for this solution to make sense, it should've been done before
		// (in a separate execution), and stored. It is done here just to ease testing.
		HotwordsBloomTrainer.train(hotwordsFile, BLOOM_FILTER_FILE);
		FileUtils.deleteDirectory(new File(outputDir));
	}
	
	private static Job createJob(String jobName, String input, String output) throws Exception
	{
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new URI(BLOOM_FILTER_FILE), conf);
		
		Job job = new Job(conf, jobName);
		job.setJarByClass(HotwordsRunner.class);

		job.setMapperClass(HotwordsMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job;
	}
}
