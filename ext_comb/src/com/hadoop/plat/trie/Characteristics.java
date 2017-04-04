package com.hadoop.plat.trie;

public class Characteristics {
    public static final long CHARACTERISTIC_NONE     = 0x0000000000000000;
    public static final long CHARACTERISTIC_VALID    = 0x0000000000000001;
    public static final long CHARACTERISTIC_DROPED   = 0x0000000000000002;
    public static final long CHARACTERISTIC_SEGMENTABLE  = 0x0000000000000004;
    public static final long CHARACTERISTIC_VALID_SEGMENTABLE = CHARACTERISTIC_VALID | CHARACTERISTIC_SEGMENTABLE;
    public static final long CHARACTERISTIC_PAYMENT  = 0x0000000000000010;
    public static final long CHARACTERISTIC_SEARCH   = 0x0000000000000020;
    public static final long CHARACTERISTIC_ASSISTANT= 0x0000000000000030;
    public static final long CHARACTERISTIC_PC       = 0x0000000000000100;
    public static final long CHARACTERISTIC_MOBILE   = 0x0000000000000200;
    public static final long CHARACTERISTIC_WAP      = 0x0000000000000400;
    
    public static final long CHARACTERISTIC_ENDWITH  = 0x0000000000010000;
    public static final long CHARACTERISTIC_EQUAL    = 0x0000000000020000;
    
    public static final long getValidSegmentable(){
        return CHARACTERISTIC_SEGMENTABLE | CHARACTERISTIC_VALID;
    }
    
    public static final boolean pc(long characteristic) {
        return (characteristic & CHARACTERISTIC_PC) != 0;
    }
    public static final boolean mobile(long characteristic) {
        return (characteristic & CHARACTERISTIC_MOBILE) != 0;
    }
    public static final boolean wap(long characteristic) {
        return (characteristic & CHARACTERISTIC_WAP) != 0;
    }
    
    public static final boolean droped(long characteristic) {
        return (characteristic & CHARACTERISTIC_DROPED) != 0;
    }
    
    public static final boolean segmentable(long characteristic) {
        return (characteristic & CHARACTERISTIC_SEGMENTABLE) != 0;
    }
    public static final boolean valid(long characteristic) {
        return (characteristic & CHARACTERISTIC_VALID) != 0;
    }
    public static final boolean validSegmentable(long characteristic){
        return valid(characteristic) && !droped(characteristic) && segmentable(characteristic);
    }
    
    public static String toString(long characteristic) {
        StringBuilder sb = new StringBuilder();
        if ((characteristic & CHARACTERISTIC_VALID) != 0)
            sb.append("VALID,");
        if ((characteristic & CHARACTERISTIC_DROPED) != 0)
            sb.append("DISCARD,");
        if ((characteristic & CHARACTERISTIC_SEGMENTABLE) != 0)
            sb.append("SEGMENT,");
        if ((characteristic & CHARACTERISTIC_PAYMENT) != 0)
            sb.append("PAY,");
        if ((characteristic & CHARACTERISTIC_PC) != 0)
            sb.append("PC,");
        if ((characteristic & CHARACTERISTIC_MOBILE) != 0)
            sb.append("MOBILE,");
        if ((characteristic & CHARACTERISTIC_WAP) != 0)
            sb.append("WAP,");
        if ((characteristic & CHARACTERISTIC_SEARCH) != 0)
            sb.append("SEARCH,");
        if ((characteristic & CHARACTERISTIC_ASSISTANT) != 0)
            sb.append("ASSISTANT,");
        if (sb.length() > 0) {
            return sb.substring(0, sb.length() - 1);
        }
        return "";
    }
}
