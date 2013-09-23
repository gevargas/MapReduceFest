import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SmartMapper extends Mapper<Object, Text, IntWritable, SortedMapWritable>
{
	private IntWritable mHour = new IntWritable(); // Key
	private IntWritable mLength = new IntWritable(); // Map Key in Value
	private static final LongWritable ONE = new LongWritable(1); // Map Value in Value

	@Override
	@SuppressWarnings("deprecation")
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		Comment comment = CommentParser.parse(value.toString());
		if (comment != null)
		{
			mHour.set(comment.getCreationDate().getHours());
			mLength.set(comment.getText().length());
			
			// Output is number of comments with this length (for the mapper, one). 
			SortedMapWritable map = new SortedMapWritable();
			map.put(mLength, ONE);

			context.write(mHour, map);
		}
	}
}