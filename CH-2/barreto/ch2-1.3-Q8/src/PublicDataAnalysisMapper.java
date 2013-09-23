import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PublicDataAnalysisMapper extends MapReduceBase implements Mapper<LongWritable, Text, PairKey, PairWritable>
{
    //map method that performs the tokenizer job and framing the initial key value pairs
    public void map(LongWritable key, Text value, OutputCollector<PairKey, PairWritable> output, Reporter reporter) throws IOException
    {
        //taking one line at a time and split it by ';'
        String line = value.toString();
        String[] split = line.split(":");
        
        //iterating through all the words available in that line and forming the key value pair
        PairKey word = new PairKey(split[0], split[4]);
        output.collect(word, new PairWritable(Double.parseDouble(split[3]), 1));
    }
}