import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PublicDataAnalysisCombiner extends MapReduceBase implements Reducer<PairKey, PairWritable, PairKey, PairWritable>
{
    //reduce method accepts the Key Value pairs from mappers, do the aggregation based on keys and produce the final out put
    public void reduce(PairKey key, Iterator<PairWritable> values, OutputCollector<PairKey, PairWritable> output, Reporter reporter) throws IOException
    {
        double sum = 0;
        int count = 0;
        /*iterates through all the values available with a key and add them together and give the
         final result as the key and sum of its values*/
        while (values.hasNext())
        {
        	PairWritable p = values.next();
            sum += p.sum;
            count += p.count;
        }
        output.collect(key, new PairWritable(sum, count));
    }
}