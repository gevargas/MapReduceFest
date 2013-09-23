import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class MedianAndStdDev implements Writable
{
	private DoubleWritable mMedian;
	private DoubleWritable mStdDev;

	public MedianAndStdDev()
	{
		this(0, 0);
	}
	
	public MedianAndStdDev(double median, double sdtDev)
	{
		mMedian = new DoubleWritable(median);
		mStdDev = new DoubleWritable(sdtDev);
	}

	public void setMedian(double median)
	{
		mMedian.set(median);
	}
	
	public void setStdDev(double stdDev)
	{
		mStdDev.set(stdDev);
	}
	
	@Override
	public boolean equals(Object obj)
	{
		if (!(obj instanceof MedianAndStdDev))
			return false;
		
		MedianAndStdDev other = (MedianAndStdDev)obj;
		return (mMedian.equals(other.mMedian) && mStdDev.equals(other.mStdDev));
	}

	@Override
	public int hashCode()
	{
		return (mMedian.hashCode() ^ mStdDev.hashCode());
	}

	@Override
	public String toString()
	{
		return String.format("Median: %s, StdDev: %s", mMedian, mStdDev);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		mMedian.write(out);
		mStdDev.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		mMedian.readFields(in);
		mStdDev.readFields(in);
	}
}
