import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistributedGrepMain extends Configured implements Tool{
    public int run(String[] args) throws Exception
    {
        //creating a JobConf object and assigning a job name for identification purposes
        Job job = new Job(getConf(), DistributedGrepMain.class.getSimpleName());
//        conf.setJobName("WordCount");
        job.setJarByClass(DistributedGrepMain.class);
        if (args.length > 2){
        	getConf().set("mapregex", args[2]);
        	job.getConfiguration().set("mapregex", args[2]);
        }else{
        	System.out.println("3 arguments must be provided. The 3'td is the regular expression to be applied.");
        	return 1;
        }
        //Setting configuration object with the Data Type of output Key and Value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        //Providing the mapper and reducer class names
        job.setMapperClass(DistributedGrepMapper.class);
        job.setNumReduceTasks(0);
        
        //the hdfs input and output directory to be fetched from the command line
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        boolean finishedOk = job.waitForCompletion(true);
        
        if (finishedOk)
        	return 0;
        else
        	return 1;
    }
    
    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new DistributedGrepMain(),args);
        System.exit(res);
    }
    
    
}