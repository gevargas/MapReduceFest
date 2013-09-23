import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PublicDataAnalysisReducer extends MapReduceBase implements Reducer<Text, PairWritable, Text, DoubleWritable>
{
    //reduce method accepts the Key Value pairs from mappers, do the aggregation based on keys and produce the final out put
    public void reduce(Text key, Iterator<PairWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException
    {
        double sum = 0;
        int count = 0;
        /*iterates through all the values available with a key and add them together and give the
         final result as the key and sum of its values*/
        while (values.hasNext())
        {	
        	PairWritable p = values.next();
            sum += p.sum;
            count+= p.count;
        }
        output.collect(key, new DoubleWritable(sum/count));
    }
}