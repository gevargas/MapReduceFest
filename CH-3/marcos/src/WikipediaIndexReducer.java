import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WikipediaIndexReducer extends Reducer<Text, Text, Text, Text> {
	private Text result = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder();
		for (Text id : values) {
			sb.append(id.toString() + " ");
		}

		result.set(sb.substring(0, sb.length() - 1).toString());
		context.write(key, result);
	}
}