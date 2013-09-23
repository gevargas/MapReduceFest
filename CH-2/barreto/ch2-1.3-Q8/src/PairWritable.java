import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class PairWritable implements Writable{
	public double sum;
	public int count;
	
	public PairWritable(){
	}
	
	public PairWritable(double sum, int count){
		this.sum = sum;
		this.count = count;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		count = in.readInt();
        sum = in.readDouble();		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(count);
        out.writeDouble(sum);
	}
	
	public static PairWritable read(DataInput in) throws IOException {
		PairWritable w = new PairWritable();
        w.readFields(in);
        return w;
      }

}
