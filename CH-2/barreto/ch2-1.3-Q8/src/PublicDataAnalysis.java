import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class PublicDataAnalysis extends Configured implements Tool{
    public int run(String[] args) throws Exception
    {
        //creating a JobConf object and assigning a job name for identification purposes
        JobConf conf = new JobConf(getConf(), PublicDataAnalysis.class);
        conf.setJobName("PublicDataAnalysis");
        
        //Setting configuration object with the Data Type of output Key and Value
        conf.setOutputKeyClass(PairKey.class);
        conf.setOutputValueClass(PairWritable.class);
        
        //Providing the mapper and reducer class names
        conf.setMapperClass(PublicDataAnalysisMapper.class);
        conf.setCombinerClass(PublicDataAnalysisCombiner.class);
        conf.setReducerClass(PublicDataAnalysisReducer.class);
        
        //the hdfs input and output directory to be fetched from the command line
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
        JobClient.runJob(conf);
        return 0;
    }
    
    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new PublicDataAnalysis(),args);
        System.exit(res);
    }
    
    
}