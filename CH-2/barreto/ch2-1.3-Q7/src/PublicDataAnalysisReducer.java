import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PublicDataAnalysisReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
    //reduce method accepts the Key Value pairs from mappers, do the aggregation based on keys and produce the final out put
    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException
    {
        double sum = 0;
        /*iterates through all the values available with a key and add them together and give the
         final result as the key and sum of its values*/
        while (values.hasNext())
        {	
        	DoubleWritable p = values.next();
            sum += p.get();
        }
        output.collect(key, new DoubleWritable(sum));
    }
}