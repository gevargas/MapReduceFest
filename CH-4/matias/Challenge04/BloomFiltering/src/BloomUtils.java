import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.hash.Hash;

public class BloomUtils
{
	public static BloomFilter newBloomFilter(int numMembers, float falsePosRate)
	{
		// Calculate vector size and optimal K value.
		int vectorSize = getOptimalBloomFilterSize(numMembers, falsePosRate);
		int nbHash = getOptimalK(numMembers, vectorSize);
		
		return new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);		
	}

	public static void saveFilterToFile(BloomFilter filter, Path bfFile) throws IOException
	{
		FileSystem fs = FileSystem.get(new Configuration());		
		FSDataOutputStream strm = fs.create(bfFile);
		filter.write(strm);
		strm.flush();
		strm.close();
	}	
	
	/**
	* Gets the optimal Bloom filter sized based on the input parameters and the
	* optimal number of hash functions.
	*
	* @param numElements The number of elements used to train the set.
	* @param falsePosRate The desired false positive rate.
	* @return The optimal Bloom filter size.
	*/
	private static int getOptimalBloomFilterSize(int numElements, float falsePosRate)
	{
		return (int) (-numElements * (float) Math.log(falsePosRate)	/ Math.pow(Math.log(2), 2));
	}
	
	/**
	* Gets the optimal-k value based on the input parameters.
	*
	* @param numElements The number of elements used to train the set.
	* @param vectorSize The size of the Bloom filter.
	* @return The optimal-k value, rounded to the closest integer.
	*/
	private static int getOptimalK(float numElements, float vectorSize)
	{
		return (int) Math.round(vectorSize * Math.log(2) / numElements);
	}	
}
