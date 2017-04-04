package com.hadoop.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SplitOutputKeyComparator extends WritableComparator {
    
    protected SplitOutputKeyComparator() {
        super(SplitOutputKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        SplitOutputKey p1 = (SplitOutputKey) w1;
        SplitOutputKey p2 = (SplitOutputKey) w2;
        return p1.compareTo(p2);
    }
}
