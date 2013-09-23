import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PublicDataAnalysisMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable>
{
    //map method that performs the tokenizer job and framing the initial key value pairs
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException
    {
        //taking one line at a time and split it by ';'
        String line = value.toString();
        String[] split = line.split(":");
        
        //iterating through all the words available in that line and forming the key value pair
        Text word = new Text(split[4]);
        output.collect(word, new DoubleWritable(Double.parseDouble(split[3])));
    }
}