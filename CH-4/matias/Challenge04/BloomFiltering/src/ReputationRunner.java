import java.io.File;
import java.io.IOException;
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

public class ReputationRunner
{
	private static final String BLOOM_FILTER_FILE = "highRepUsers.bloom"; 
	public static final int REPUTATION_THRESHOLD = 1500;

	public static void main(String[] args) throws Exception
	{
		// For debugging.
//		if (args.length == 0)
//		{
//			args = new String[] { 
//				"/home/matiash/MapReduceFest/hadoop-1.0.4/ch4/inputMetaSciFi", 
//				"/home/matiash/MapReduceFest/hadoop-1.0.4/ch4/outputReputationMetaSciFi", 
//				"/home/matiash/MapReduceFest/hadoop-1.0.4/ch4/UsersMetaSciFi.xml" };
//		}

		// To ease testing, this Runner does everything:
		// 1) Loads the database from the users file.
		// 2) Trains the bloom filter from the same file.
		// 3) Runs the Map/Reduce tasks.
		// To make more sense, 1 & 2 should be performed beforehand, and their results stored.
		if (args.length < 3)
		{
			System.err.println("Usage: ReputationRunner <inputDir> <outputDir> <usersFile>");
			System.exit(1);
		} 

		// Load HBase and train bloom filter.
		setup(args[2], REPUTATION_THRESHOLD, args[1]);

		// Run Map/Reduce.
		Job job = createJob("Runner", args[0], args[1]);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	private static void setup(String usersFile, int reputationTreshold, String outputDir) throws IOException
	{
		ReputationSetup.setup(usersFile, BLOOM_FILTER_FILE, reputationTreshold);
		FileUtils.deleteDirectory(new File(outputDir));
	}

	private static Job createJob(String jobName, String input, String output) throws Exception
	{
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new URI(BLOOM_FILTER_FILE), conf);
		
		Job job = new Job(conf, jobName);
		job.setJarByClass(ReputationRunner.class);

		job.setMapperClass(ReputationMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job;
	}
	
}
