import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

public class SmartReducer extends Reducer<IntWritable, SortedMapWritable, IntWritable, MedianAndStdDev>
{
	private TreeMap<Integer, Long> commentLengthCounts = new TreeMap<Integer, Long>();

	@Override
	@SuppressWarnings("rawtypes")
	public void reduce(IntWritable key, Iterable<SortedMapWritable> values,	Context context) throws IOException, InterruptedException
	{
		double sum = 0;
		long totalComments = 0;
		commentLengthCounts.clear();
		MedianAndStdDev result = new MedianAndStdDev();
		for (SortedMapWritable v : values)
		{
			for (Map.Entry<WritableComparable, Writable> entry : v.entrySet())
			{
				int length = ((IntWritable) entry.getKey()).get();
				long count = ((LongWritable) entry.getValue()).get();
				totalComments += count;
				sum += length * count;
				Long storedCount = commentLengthCounts.get(length);

				if (storedCount == null)
				{
					commentLengthCounts.put(length, count);
				}
				else
				{
					commentLengthCounts.put(length, storedCount + count);
				}
			}
			
			v.clear(); // IMPORTANT! Workaround for https://issues.apache.org/jira/browse/HADOOP-5454 
		}

		long medianIndex = totalComments / 2L;
		long previousComments = 0;
		long comments = 0;
		int prevKey = 0;

		for (Map.Entry<Integer, Long> entry : commentLengthCounts.entrySet())
		{
			comments = previousComments + entry.getValue();
			if (previousComments <= medianIndex && medianIndex < comments)
			{
				if (totalComments % 2 == 0 && previousComments == medianIndex)
				{
					result.setMedian((double) (entry.getKey() + prevKey) / 2.0f);
				}
				else
				{
					result.setMedian(entry.getKey());
				}
				break;
			}
			previousComments = comments;
			prevKey = entry.getKey();
		}

		// calculate standard deviation
		double mean = sum / totalComments;
		double sumOfSquares = 0.0d;
		for (Map.Entry<Integer, Long> entry : commentLengthCounts.entrySet())
		{
			sumOfSquares += (entry.getKey() - mean) * (entry.getKey() - mean) * entry.getValue();
		}
		
		result.setStdDev((double) Math.sqrt(sumOfSquares / totalComments));
		context.write(key, result);
	}
}