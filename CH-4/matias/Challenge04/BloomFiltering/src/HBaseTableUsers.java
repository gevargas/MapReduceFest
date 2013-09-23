import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTableUsers
{
	private final Configuration mConf;
	private HTable mTable;
	
	private static final byte[] TABLE = Bytes.toBytes("user_table");
	private static final byte[] COLUMN_FAMILY = Bytes.toBytes("attr");
	private static final byte[] USER_NAME = Bytes.toBytes("UserName");
	private static final byte[] USER_REPUTATION = Bytes.toBytes("Reputation");

	public HBaseTableUsers() throws IOException
	{
		this(getHBaseConfiguration());
	}

	private HBaseTableUsers(Configuration conf) throws IOException
	{
		mConf = conf;
	}	
	
	private static Configuration getHBaseConfiguration() throws IOException
	{
		Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("hbase.master", "localhost:60000");
        
        HBaseAdmin.checkHBaseAvailable(conf);
        return conf;
	}

	public void close() throws IOException
	{
		if (mTable != null)
			mTable.close();
	}
	
	void createTable() throws IOException
	{
		HBaseAdmin admin = new HBaseAdmin(mConf);
		
		// Create the "user_table" table, deleting it if necessary. 
		if (admin.tableExists(TABLE))
		{
			if (admin.isTableEnabled(TABLE))
				admin.disableTable(TABLE);
			
			admin.deleteTable(TABLE);
		}

		HTableDescriptor descriptor = new HTableDescriptor(TABLE);
		descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
		admin.createTable(descriptor);
	}
	
	public void insertUser(UserData user) throws IOException
	{
		initTable();
		Put put = new Put(Bytes.toBytes(user.Id));
		put.add(COLUMN_FAMILY, USER_NAME, Bytes.toBytes(user.Name));
		put.add(COLUMN_FAMILY, USER_REPUTATION, Bytes.toBytes(user.Reputation));
		mTable.put(put);		
	}
	
	public UserData getUser(int userId) throws IOException
	{
		initTable();
		Result r = mTable.get(new Get(Bytes.toBytes(userId)));
		
		String userName = Bytes.toString(r.getValue(COLUMN_FAMILY, USER_NAME));
		int reputation = Bytes.toInt(r.getValue(COLUMN_FAMILY, USER_REPUTATION));
		
		return new UserData(userId, userName, reputation);
	}
	
	private void initTable() throws IOException
	{
		if (mTable == null)
			mTable = new HTable(mConf, TABLE);		
	}
}
