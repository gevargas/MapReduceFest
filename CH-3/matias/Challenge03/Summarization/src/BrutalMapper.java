import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BrutalMapper extends Mapper<Object, Text, IntWritable, IntWritable>
{
	private IntWritable mHour = new IntWritable(); // Key
	private IntWritable mLength = new IntWritable(); // Value
	
	@Override
	@SuppressWarnings("deprecation")
	protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException
	{
		Comment comment = CommentParser.parse(value.toString());
		if (comment != null)
		{
			mHour.set(comment.getCreationDate().getHours());
			mLength.set(comment.getText().length());
			
			context.write(mHour, mLength);
		}
	}
}
