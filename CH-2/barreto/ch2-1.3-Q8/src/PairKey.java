import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class PairKey implements WritableComparable{
	public String category;
	public String month;
	
	public PairKey(){
	}
	
	public PairKey(String cat, String month){
		this.category = cat;
		this.month = month;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		category = in.readUTF();
        month = in.readUTF();	
        System.out.println("category: "+category!=null?category:"null");
        System.out.println("month: "+month!=null?month:"null");
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(category);
        out.writeUTF(month);
	}
	
	public int compareTo(Object o) {
		PairKey w = (PairKey)o;
        int res = this.month.compareTo(w.month);
        if (res != 0) return res;
        else{
        	return this.category.compareTo(w.category);
        }
      }
}
