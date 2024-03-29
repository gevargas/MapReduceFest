import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistinctUserReducer extends Reducer<Text, NullWritable, Text, NullWritable>
{
	public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
	{
		// Write the user's id with a null value.
		context.write(key, NullWritable.get());
	}
}