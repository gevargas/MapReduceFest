import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CartesianMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>
{
	private Text outkey = new Text();
	private Text outvalue = new Text();

	@Override
	public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
	{
		Map<String, String> left = Utils.transformXmlToMap(key.toString());
		Map<String, String> right = Utils.transformXmlToMap(value.toString());
		
		String leftText = left.get("Text");
		String rightText = right.get("Text");
		
		if (leftText != null && rightText != null && !leftText.equals(rightText))
		{
			String[] leftTokens = leftText.split("\\s");
			String[] rightTokens = rightText.split("\\s");
			HashSet<String> leftSet = new HashSet<String>(Arrays.asList(leftTokens));
			HashSet<String> rightSet = new HashSet<String>(Arrays.asList(rightTokens));

			int sameWordCount = 0;
			StringBuilder words = new StringBuilder();
			for (String s : leftSet)
			{
				if (rightSet.contains(s))
				{
					words.append(s + ",");
					++sameWordCount;
				}
			}
			
			// If there are at least three words, output
			if (sameWordCount > 2)
			{
//				outkey.set(words + "\t" + leftText);
//				outvalue.set(rightText);

				// For writing less.
				String leftId = left.get("Id");
				String rightId = right.get("Id");
				outkey.set(words + "\t" + leftId);
				outvalue.set(rightId);
						
				output.collect(outkey, outvalue);
			}
		}
	}
}
