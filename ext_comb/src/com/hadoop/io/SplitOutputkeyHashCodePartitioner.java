package com.hadoop.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SplitOutputkeyHashCodePartitioner extends Partitioner<SplitOutputKey, Text> {
    
    @Override
    public int getPartition(SplitOutputKey key, Text arg1, int numPartitions) {
        int partitionNum = 0;
        partitionNum = (int) (( key.partitionHashCode() & Integer.MAX_VALUE) % numPartitions);
        return partitionNum;
    }
}
