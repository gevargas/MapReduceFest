import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

public class SmartCombiner extends Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable>
{
	@SuppressWarnings("rawtypes")
	@Override
	protected void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException
	{
		SortedMapWritable outValue = new SortedMapWritable();
		for (SortedMapWritable v : values)
		{
			for (Map.Entry<WritableComparable, Writable> entry : v.entrySet())
			{
				LongWritable count = (LongWritable) outValue.get(entry.getKey());
				if (count != null)
				{
					count.set(count.get() + ((LongWritable) entry.getValue()).get());
				}
				else
				{
					outValue.put(entry.getKey(), new LongWritable(((LongWritable) entry.getValue()).get()));
				}
			}
			
			v.clear(); // IMPORTANT! Workaround for https://issues.apache.org/jira/browse/HADOOP-5454
		}
		
		context.write(key, outValue);
	}
}