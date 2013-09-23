import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Naive reducer (adapted from MapReduce Patterns book).
 * @author matiash
 */
public class BrutalReducer extends Reducer<IntWritable, IntWritable, IntWritable, MedianAndStdDev>
{
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		ArrayList<Integer> lengths = new ArrayList<Integer>();

		// Iterate through all comment lengths for this hour.
		// Add them to a list (to sort and find median) and calculate sum (for standard deviation).
		double sum = 0;
		for (IntWritable value : values)
		{
			lengths.add(value.get());
			sum += value.get();
		}
		
		Collections.sort(lengths);
		int count = lengths.size();
		double mean = sum / count;

		// Find median (middle value or average of the two middle values).
		double median;
		if (count % 2 == 0)
			median = (lengths.get(count / 2 - 1) + lengths.get(count / 2)) / 2.0f;
		else
			median = lengths.get(count / 2);

		// Calculate standard deviation
		double sumOfSquares = 0.0f;
		for (int length : lengths)
			sumOfSquares += (length - mean) * (length - mean);

		double stdDev = Math.sqrt(sumOfSquares / count);

		// Key is hour. Value is mean and standard deviation.
		MedianAndStdDev result = new MedianAndStdDev(median, stdDev);
		context.write(key, result);
	}
}
