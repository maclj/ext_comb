package com.hadoop.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

@Deprecated
public class SplitTextHashCodePartitioner extends Partitioner<SplitText, Text> {
    
    @Override
    public int getPartition(SplitText key, Text arg1, int numPartitions) {
        int partitionNum = 0;
        partitionNum = (int) (( key.hashCode() & Integer.MAX_VALUE) % numPartitions);
        return partitionNum;
    }
}
