import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
	//map for saving intermediate map values.
	private Map<String, Integer> map;
	
	
	// map method that performs the tokenizer job and framing the initial key
	// value pairs
	public void map(Object key, Text value, Context context) 
			  throws IOException, InterruptedException {
		// taking one line at a time and tokenizing the same
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);

		// iterating through all the words available in that line and forming
		// the key value pair
		while (tokenizer.hasMoreTokens()) {
			String token =  tokenizer.nextToken();
			int count = 1;
			if (map.containsKey(token)){
				count += map.get(token);
			}
			map.put(token, count);
		}
	}

	protected void cleanup(
			Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		for(String key: map.keySet()){
			int count = map.get(key);
			context.write(new Text(key), new IntWritable(count));
		}
		
	}

	protected void setup(
			org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		map = new HashMap<String, Integer>();
	}

}