import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;



public class PairWritable implements WritableComparable<PairWritable>
{
	public long source;
	public long sink;

	public PairWritable (long in, long out)
	{
		this.source = in;
		this.sink = out;
	}

	public void write(DataOutput out) throws IOException
	{
		out.writeLong(source);
		out.writeLong(sink);
	}

	public void readFields(DataInput in) throws IOException
	{
		source = in.readLong();
		sink = in.readLong();
	}

	public String toString()
	{
    	return Long.toString(source) + ", " + Long.toString(sink);
    }

    public int compareTo(PairWritable other)
    {
    	if(this.source == other.source)
    	{
    		return (this.sink < other.sink) ? -1 : ((this.sink == other.sink) ? 0 : 1);
    	}
    	else
    	{
    		return (this.source < other.source) ? -1 : ((this.source == other.source) ? 0 : 1);
    	}
    }


}