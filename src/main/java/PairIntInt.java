import javafx.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PairIntInt implements WritableComparable {
    private IntWritable first;
    private IntWritable second;

    public PairIntInt(){
        first = new IntWritable(0);
        second = new IntWritable(0);
    }
    public PairIntInt(IntWritable a, IntWritable b){
        this.first = a;
        this.second = b;
    }

    public IntWritable first() {
        return first;
    }

    public IntWritable second() {
        return second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PairIntInt that = (PairIntInt) o;

        if (!Objects.equals(first, that.first)) return false;
        if (!Objects.equals(second, that.second)) return false;

        return true;
    }

    @Override
    public int compareTo(Object o) {
        PairIntInt other = (PairIntInt) o;
        int firstCompare = this.first.compareTo(other.first);
        return firstCompare != 0 ? firstCompare : this.second.compareTo(other.second);
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
        return first.get() + "\t" + second.get();
    }
}
