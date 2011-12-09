import org.apache.hadoop.io.ArrayWritable;


public class PairArrayWritable extends ArrayWritable
{
	public PairArrayWritable() {
        super(PairWritable.class);
    }
    
    public PairArrayWritable(PairWritable[] values) {
        super(PairWritable.class, values);
    }
}