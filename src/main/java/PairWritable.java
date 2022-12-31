import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PairWritable <T extends WritableComparable, S extends WritableComparable> implements WritableComparable {
    public T first;
    public S second;


    public PairWritable(T a, S b){
        this.first = a;
        this.second = b;
    }

    public T first() {
        return first;
    }

    public S second() {
        return second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PairWritable that = (PairWritable) o;

        if (!Objects.equals(first, that.first)) return false;
        if (!Objects.equals(second, that.second)) return false;

        return true;
    }

    @Override
    public int compareTo(Object o) {
        PairWritable other = (PairWritable) o;
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
}
