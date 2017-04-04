package com.hadoop.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

@Deprecated
public class SplitTextGroupingComparator extends WritableComparator {
    
    public SplitTextGroupingComparator() {
        super(SplitText.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable o1, WritableComparable o2) {
        SplitText p1 = (SplitText) o1;
        SplitText p2 = (SplitText) o2;
        return p1.compareTo(p2);
    }
}
