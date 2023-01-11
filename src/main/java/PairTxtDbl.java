import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PairTxtDbl implements WritableComparable {
    private Text first;
    private DoubleWritable second;

    public PairTxtDbl(){
        first = new Text("");
        second = new DoubleWritable(0);
    }
    public PairTxtDbl(Text a, DoubleWritable b){
        this.first = a;
        this.second = b;
    }

    public Text first() {
        return first;
    }

    public DoubleWritable second() {
        return second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PairTxtDbl that = (PairTxtDbl) o;

        if (!Objects.equals(first, that.first)) return false;
        if (!Objects.equals(second, that.second)) return false;

        return true;
    }

    @Override
    public int compareTo(Object o) {
        PairTxtDbl other = (PairTxtDbl) o;
        int firstCompare = this.first.compareTo(other.first);
        return firstCompare != 0 ? firstCompare : -1 * this.second.compareTo(other.second);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    @Override
    public String toString() {
        return first.toString() + "\t" + String.format("%.12f",second.get());
    }
}
