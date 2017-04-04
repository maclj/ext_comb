package com.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *  二次排序移植。 
 */
public class SplitOutputKey implements SplitKey, WritableComparable<SplitOutputKey> {

    /** 新增的拆分key */
    private int splitKey = 0;
    
    /** 业务需要的key value */
    private String key = "";
    
    /** 排序字段 */
    private long order = 0l;

    public SplitOutputKey() {
    }

    public SplitOutputKey(String key) {
        this.key = key;
        this.order = 0l;
    }

    public SplitOutputKey(String key, long order) {
        this.key = key;
        this.order = order;
    }
    
    public SplitOutputKey(int splitKey, String key, long order) {
        this.splitKey = splitKey;
        this.key = key;
        this.order = order;
    }

    public String getKey() {
        return this.key;
    }

    public long getOrder() {
        return this.order;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setOrder(long order) {
        this.order = order;
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
        this.order = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.splitKey);
        out.writeUTF(key);
        out.writeLong(this.order);
    }

    @Override
    public int compareTo(SplitOutputKey other) {
        int result = this.splitKey - other.splitKey;
        if(result != 0) {
            return result > 0 ? 1 : -1;
        }
        
        result = this.key.compareTo(other.key);
        if (result == 0) {
            long tmp = this.order - other.order;
            return tmp > 0 ? 1 : (tmp == 0 ? 0 : -1) ;
        }
        return result;
    }
    
    public boolean equals(Object other){
        return (other instanceof SplitOutputKey) && compareTo((SplitOutputKey) other) == 0;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.splitKey, this.key, this.order);
    }
    
    /**
     * 分区使用的hashCode值。
     * @return
     */
    public int partitionHashCode() {
        return Objects.hash(this.splitKey, this.key);
    }
    
    /**
     * 分组使用的比较器。
     * @return
     */
    public int groupCompareTo(SplitOutputKey other) {
        int result = this.splitKey - other.splitKey;
        if(result != 0) {
            return result > 0 ? 1 : -1;
        }
        
        result = this.key.compareTo(other.key);
        return result > 0 ? 1 : (result == 0 ? 0 : -1) ;
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
        WritableComparator.define(SplitOutputKey.class, new SplitOutputKeyComparator());
    }

}
