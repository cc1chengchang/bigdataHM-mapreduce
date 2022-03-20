import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class phoneTraffic implements Writable {
    private long upStream;
    private long downStream;
    private long sum;

    public phoneTraffic () {};

    public phoneTraffic (long upStream, long downStream, long sum) {
        this.upStream = upStream;
        this.downStream = downStream;
        this.sum = sum;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upStream);
        dataOutput.writeLong(downStream);
        dataOutput.writeLong(sum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upStream = dataInput.readLong();
        this.downStream = dataInput.readLong();
        this.sum = dataInput.readLong();
    }

    public long getUpstream() {
        return upStream;
    }

    public void setUpstream(long upStream) {
        this.upStream  = upStream;
    }

    public long getDownstream() {
        return downStream;
    }

    public void setDownstream(long downStream) {
        this.downStream  = downStream;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum  = sum;
    }

    @Override
    public String toString() {
        return upStream + "\t" + downStream + "\t" + sum;
    }
}
