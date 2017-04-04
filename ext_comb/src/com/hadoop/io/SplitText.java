package com.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *  支持拆分key的Text版本。
 *  未实现，请勿使用。  
 */
@Deprecated
public class SplitText implements SplitKey, WritableComparable<SplitText> {

    /** 拆分key */
    private int splitKey = 0;
    
    /** 业务value  */
    private String key = "";

    public SplitText() {
    }

    public SplitText(String key) {
        this.key = key;
    }

    public SplitText(int splitKey, String key) {
        this.key = key;
        this.splitKey = splitKey;
    }
    

    public String getKey() {
        return this.key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public int getSplitKey() {
        return splitKey;
    }

    @Override
    public void setSplitKey(int splitKey) {
        this.splitKey = splitKey;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.splitKey = in.readInt();
        this.key = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.splitKey);
        out.writeUTF(key);
    }

    @Override
    public int compareTo(SplitText other) {
        int result = this.splitKey - other.splitKey;
        if(result != 0) {
            return result > 0 ? 1 : -1;
        }
        
        result = this.key.compareTo(other.key);
        return result;
    }
    
    @Override
    public boolean equals(Object other){
        return (other instanceof SplitText) && compareTo((SplitText) other) == 0;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.splitKey, this.key);
    }

    @Override
    public String toString() {
    	return this.getKey();
    }
    
    @Override
    public String toReal() {
        return this.getKey();
    }

    static { // register this comparator
        WritableComparator.define(SplitText.class, new SplitTextComparator());
    }
}
