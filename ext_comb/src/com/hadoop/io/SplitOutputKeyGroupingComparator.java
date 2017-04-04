package com.hadoop.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SplitOutputKeyGroupingComparator extends WritableComparator {
    
    public SplitOutputKeyGroupingComparator() {
        super(SplitOutputKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable o1, WritableComparable o2) {
        SplitOutputKey p1 = (SplitOutputKey) o1;
        SplitOutputKey p2 = (SplitOutputKey) o2;
        return p1.groupCompareTo(p2);
    }
}
