import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

public class ReputationSetup
{
	private static final float BLOOM_FALSE_POSITIVE_RATE = 0.001f;
	
	public static void setup(String usersFile, String bloomFilterFile, int reputationTreshold) throws IOException
	{
		// Create "users" table.
		HBaseTableUsers table = new HBaseTableUsers();
		table.createTable();
		
		// Insert data from all users into database, and collect ids of those
		// with reputation greater than threshold.
		FileInputStream inputStream = new FileInputStream(usersFile);
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
		LinkedList<Integer> highRepUsers = new LinkedList<Integer>();

		for (String line = reader.readLine(); line != null; line = reader.readLine())
		{
			Map<String, String> userAtts = Utils.transformXmlToMap(line);
			Integer userId = Utils.getIntProperty(userAtts, "Id");
			if (userId != null && userId > 0)
			{
				String userName = userAtts.get("DisplayName");
				int userReputation = Integer.parseInt(userAtts.get("Reputation"));
				UserData userData = new UserData(userId, userName, userReputation);
				
				table.insertUser(userData);
				
				if (userData.Reputation >= reputationTreshold)
					highRepUsers.add(userData.Id);
			}
		}
		
		// Now train the bloom filter.
		BloomFilter highRepFilter = BloomUtils.newBloomFilter(highRepUsers.size(), BLOOM_FALSE_POSITIVE_RATE); 
		for (Integer highRepUserId : highRepUsers)
			highRepFilter.add(new Key(Bytes.toBytes(highRepUserId)));
		
		reader.close();
		BloomUtils.saveFilterToFile(highRepFilter, new Path(bloomFilterFile));
	}	
}
