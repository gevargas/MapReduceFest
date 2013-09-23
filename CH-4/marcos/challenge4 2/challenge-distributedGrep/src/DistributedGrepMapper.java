import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributedGrepMapper extends Mapper<Object, Text, Text, NullWritable> {
	private String mapRegex;
	private Pattern pattern;

	public void setup(Context context) throws IOException, InterruptedException {
		mapRegex = context.getConfiguration().get("mapregex");
		pattern = Pattern.compile(mapRegex);
		
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		Matcher m =pattern.matcher(value.toString()); 
		if (m.find()) {
			context.write(value, NullWritable.get());
		}
	}
}
