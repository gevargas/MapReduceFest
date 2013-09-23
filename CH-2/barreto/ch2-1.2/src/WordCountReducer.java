import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
	
	
	//reduce method accepts the Key Value pairs from mappers, do the aggregation based on keys and produce the final out put
	protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text,IntWritable,Text,IntWritable>.Context context) throws IOException ,InterruptedException {
        int sum = 0;
        
        Iterator<IntWritable> it = values.iterator();
        
        /*iterates through all the values available with a key and add them together and give the
         final result as the key and sum of its values*/
        while (it.hasNext())
        {
            sum += it.next().get();
        }
        context.write(key, new IntWritable(sum));
    }
}