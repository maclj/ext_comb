package com.hadoop.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

@Deprecated
public class SplitTextComparator extends WritableComparator {
    
    protected SplitTextComparator() {
        super(SplitText.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        SplitText p1 = (SplitText) w1;
        SplitText p2 = (SplitText) w2;
        return p1.compareTo(p2);
    }
}
