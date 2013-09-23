import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCount extends Configured implements Tool{
    public int run(String[] args) throws Exception
    {
        //creating a JobConf object and assigning a job name for identification purposes
        Job job = new Job(getConf(), WordCount.class.getSimpleName());
//        conf.setJobName("WordCount");
        job.setJarByClass(WordCount.class);
        
        
        //Setting configuration object with the Data Type of output Key and Value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        //Providing the mapper and reducer class names
        job.setMapperClass(WordCountMapper.class);
//        conf.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        
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
        int res = ToolRunner.run(new Configuration(), new WordCount(),args);
        System.exit(res);
    }
    
    
}