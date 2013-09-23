import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

public class ReputationMapper extends Mapper<Object, Text, Text, NullWritable>
{
	private BloomFilter mFilter = new BloomFilter();
	private HBaseTableUsers mTable;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		// Get bloom filter from the Distributed Cache.
		URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());

		// Read bloom filter.
		DataInputStream strm = new DataInputStream(new FileInputStream(files[0].getPath()));
		mFilter.readFields(strm);
		strm.close();
		
		// Get HBase table of user info.
		mTable = new HBaseTableUsers();
	}
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		Map<String, String> comment = Utils.transformXmlToMap(value.toString());

		// Get the value for the comment
		Integer userId = Utils.getIntProperty(comment, "UserId");
		if (userId != null)
		{
			// If this user ID is in the set
			if (mFilter.membershipTest(new Key(Bytes.toBytes(userId))))
			{
				// Get the UserData from the HBase table.
				UserData userData = mTable.getUser(userId);

				// If the reputation is at least 1500, write the record to the file system
				if (userData.Reputation >= ReputationRunner.REPUTATION_THRESHOLD)
					context.write(value, NullWritable.get());
				else
					System.err.println("False positive!");
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		mTable.close();
		super.cleanup(context);
	}
}