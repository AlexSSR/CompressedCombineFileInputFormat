package main.java.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


/**
 * This record keeps filename,offset pairs.
 */

@SuppressWarnings("rawtypes")

public class CompressedCombineFileWritable implements WritableComparable {

    public long offset;
    public String fileName;



    public CompressedCombineFileWritable() {
        super();
    }

    public CompressedCombineFileWritable(long offset, String fileName) {
        super();
        this.offset = offset;
        this.fileName = fileName;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.offset = in.readLong();
        this.fileName = Text.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(offset);
        Text.writeString(out, fileName);
    }


    @Override
    public int compareTo(Object o) {
        CompressedCombineFileWritable that = (CompressedCombineFileWritable)o;

        int f = this.fileName.compareTo(that.fileName);
        if(f == 0) {
            return (int)Math.signum((double)(this.offset - that.offset));
        }
        return f;
    }
    @Override
    public boolean equals(Object obj) {
        if(obj instanceof CompressedCombineFileWritable) {
            return this.compareTo(obj) == 0;
        }
        return false;
    }
    @Override
    public int hashCode() {

        final int hashPrime = 47;
        int hash = 13;
        hash =   hashPrime* hash + (this.fileName != null ? this.fileName.hashCode() :0);
        hash =  hashPrime* hash + (int) (this.offset ^ (this.offset >>> 16));

        return hash;
    }
    @Override
    public String toString(){
        return this.fileName+"-"+this.offset;
    }
}

