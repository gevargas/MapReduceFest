import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

public class HotwordsMapper extends Mapper<Object, Text, Text, NullWritable>
{
	private BloomFilter filter = new BloomFilter();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		// Get file from the DistributedCache
		URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());

		// Read into our Bloom filter.
		DataInputStream strm = new DataInputStream(new FileInputStream(files[0].getPath()));
		filter.readFields(strm);
		strm.close();
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		Map<String, String> parsed = Utils.transformXmlToMap(value.toString());

		// Get the value for the comment
		String comment = parsed.get("Text");
		if (comment != null)
		{
			StringTokenizer tokenizer = new StringTokenizer(comment);
	
			// For each word in the comment
			while (tokenizer.hasMoreTokens())
			{
				// If the word is in the filter, output the record and break.
				String word = Utils.getCleanWord(tokenizer.nextToken());
				if (word.length() != 0)
				{
					if (filter.membershipTest(new Key(word.getBytes())))
					{
						context.write(value, NullWritable.get());
						break;
					}
				}
			}
		}
	}
}
