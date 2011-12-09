import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;



public class LongArrayWritable extends ArrayWritable
{
    public LongArrayWritable() {
        super(LongWritable.class);
    }
    
    public LongArrayWritable(LongWritable[] values) {
        super(LongWritable.class, values);
    }
}