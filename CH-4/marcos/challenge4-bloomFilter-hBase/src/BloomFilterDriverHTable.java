import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class BloomFilterDriverHTable {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		createHBaseUserReputation(args, conf);
		
		System.exit(0);
	}

	/**
	 * Creates de HBase with stackoverflow user reputations.
	 * 
	 * @throws IOException
	 * 
	 */
	private static void createHBaseUserReputation(String[] args,
			Configuration conf) throws IOException {
		Path users = new Path(args[0]);
		Path bfFile = new Path(args[1]);
		
		String line = null;
		FileSystem fs = FileSystem.get(conf);
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable htable = new HTable(hbaseConfig, "user_table");
		htable.setAutoFlush(false);
		htable.setWriteBufferSize(1024 * 1024 * 12);

		BufferedReader rdr = new BufferedReader(new InputStreamReader(
				fs.open(users)));

		
		int totalRecords = 100000;
		//BLOOM FILTER
		int vectorSize = getOptimalBloomFilterSize(totalRecords, 0.1f);
		int nbHash = getOptimalK(totalRecords, vectorSize);
		// Create new Bloom filter
		BloomFilter filter = new BloomFilter(vectorSize, nbHash,
				Hash.MURMUR_HASH);
		
		int flushCount = 0;
		
		while ((line = rdr.readLine()) != null) {
			// Parse the input into a nice map.
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(line);
			// Get the value for the comment
			String userid = parsed.get("Id");
			if (userid != null) {
				String reputation = parsed.get("Reputation");
				if (Integer.valueOf(reputation) >= 1500){
					filter.add(new Key(userid.getBytes()));
				}
				Put put = new Put(userid.getBytes());
				put.add("data".getBytes(), "reputation".getBytes(), reputation.getBytes());
				htable.put(put);
//				flushCount++;
//				if ((flushCount % 100) == 0){
//					htable.flushCommits();
//				}
			}
		}
		
		// PREVIOUS CODE
		System.out.println("Serializing Bloom filter to HDFS at " + bfFile);
		FSDataOutputStream strm = fs.create(bfFile);
		filter.write(strm);
		strm.flush();
		strm.close();

		// SAVE HOTLIST.
		DistributedCache.addCacheFile(fs.makeQualified(bfFile).toUri(), conf);

		htable.flushCommits();
		htable.close();
		System.out.println("done");

		/*
		 * <row Id="4" Reputation="5716" CreationDate="2009-06-27T00:00:00.000"
		 * DisplayName="Joel Spolsky"
		 * EmailHash="d77663c217e9d27b725338c06674c94b"
		 * LastAccessDate="2010-11-04T20:39:07.077"
		 * WebsiteUrl="http://www.joelonsoftware.com/" Location="New York, NY"
		 * AboutMe="Co-founder of Stack Overflow" Views="1069" UpVotes="76"
		 * DownVotes="14" />
		 */
	}

	public static int getOptimalBloomFilterSize(int numRecords,
			float falsePosRate) {
		int size = (int) (-numRecords * (float) Math.log(falsePosRate) / Math
				.pow(Math.log(2), 2));
		return size;
	}

	public static int getOptimalK(float numMembers, float vectorSize) {
		return (int) Math.round(vectorSize / numMembers * Math.log(2));
	}

}