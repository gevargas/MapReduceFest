import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

public class HotwordsBloomTrainer
{
	public static final float FALSE_POSITIVE_RATE = 0.001f;
	
	public static void train(String wordsFile, String bfFile) throws IOException
	{
		// Read the list of hotwords.
		FileInputStream fileStream = new FileInputStream(wordsFile);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fileStream, "UTF-8"));

		List<String> words = new ArrayList<String>();
		for (String line = reader.readLine(); line != null; line = reader.readLine())
			words.add(line);

		reader.close();
        
        // Train the Bloom Filter and store it on HDFS. 
		BloomFilter filter = trainFilter(words);
		BloomUtils.saveFilterToFile(filter, new Path(bfFile));
	}
	
	private static BloomFilter trainFilter(List<String> hotWords)
	{
		int numMembers = hotWords.size();

		// Create new Bloom filter from hot-words array.
		BloomFilter filter = BloomUtils.newBloomFilter(numMembers, FALSE_POSITIVE_RATE);
		for (String hotWord : hotWords)
			filter.add(new Key(hotWord.getBytes()));
		
		return filter;
	}
}