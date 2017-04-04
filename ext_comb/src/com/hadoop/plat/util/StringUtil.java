package com.hadoop.plat.util;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

/**
 * 需要持续积累的常见字符处理工具。
 * 
 * 
 *
 */
public class StringUtil {

    /**
     * 根据索引查找指定分隔符之前的字符串
     * 
     * @param value
     * @param split
     * @param index
     * @return
     */
    public static String find(String value, String split, int index) {
        if (value == null || split == null || index < 0) {
            return null;
        }
        value = value.trim();
        if (value.length() == 0) {
            return null;
        }
        int counter = 0;
        int slen = split.length();
        int beginIndex = 0;
        int endIndex = 0;
        while ((endIndex = value.indexOf(split, beginIndex)) >= 0) {
            if (counter++ == index) {
                if ((endIndex - beginIndex) == 0) {
                    return "";
                }
                return value.substring(beginIndex, endIndex);
            }
            // 过滤掉仅有分隔符，但是没有内容的情况。如连续的||
            if ((endIndex - beginIndex) == 0) {
                beginIndex += slen;
                continue;
            }
            beginIndex += (endIndex - beginIndex);// tmp.length();
            beginIndex += slen;
        }
        return null;
    }

    /**
     * 优化拆分性能，取消正则表达式处理。 提升不大，5000w次拆分提升2s
     * 
     * @param value
     * @param split
     * @return
     */
    public static String[] split(String value, String split) {
        if (value == null || split == null) {
            return null;
        }
        value = value.trim();
        if (value.length() == 0) {
            return null;
        }
        List<String> result = new ArrayList<String>();
        int slen = split.length();
        int beginIndex = 0;
        int endIndex = 0;
        String tmp = null;
        while ((endIndex = value.indexOf(split, beginIndex)) >= 0) {
            // 过滤掉仅有分隔符，但是没有内容的情况。如连续的||
            if ((endIndex - beginIndex) == 0) {
                beginIndex += slen;
                continue;
            }
            tmp = value.substring(beginIndex, endIndex);
            result.add(tmp);
            beginIndex += tmp.length();
            beginIndex += slen;
        }
        if (endIndex < 0) {
            // 没有匹配上结束分隔符时，将剩下的内容全部加入。
            tmp = value.substring(beginIndex);
            if (tmp.length() > 0) { // 没有做trim处理，空值不会被加入，但是连续空格仍然会被加入。
                result.add(tmp);
            }
        }
        return result.toArray(new String[] {});
    }

    /**
     * 只分隔出指定长度的数组。
     * 
     * @param str
     * @param plainSeperator
     * @param len
     * @return
     */
    public static List<String> fastSplitToLimit(String str, String plainSeperator, int len) {

        List<String> result = new ArrayList<String>(len);
        int pos = 0;
        int idx = 0;
        int inc = plainSeperator.length();
        int size = 0;
        while (true) {
            idx = str.indexOf(plainSeperator, pos);
            if (idx < 0) {
                result.add(str.substring(pos, str.length()));
                break;
            }
            // 达到分隔限制，则直接返回剩余内容
            if (++size == len) {
                result.add(str.substring(pos));
                break;
            }
            ;
            result.add(str.substring(pos, idx));
            pos = idx + inc;

        }
        return result;
    }

    /**
     * 按一个文本（非正则表达式）分隔字符串
     * 
     * @param str
     * @param plainSeperator
     * @param len
     * @return
     * @since 1.0
     */
    public static List<String> fastSplit(String str, String plainSeperator, int len) {
        List<String> ls = new ArrayList<String>(len);
        return fastSplit(str, plainSeperator, 0, ls);
    }

    /**
     * 按一个文本（非正则表达式）分隔字符串
     * 
     * @param str
     * @param plainSeperator
     * @param start
     *            开始位置
     * @param len
     * @return
     * @since 1.0
     */
    public static List<String> fastSplit(String str, String plainSeperator, int len, int start) {
        List<String> ls = new ArrayList<String>(len);
        return fastSplit(str, plainSeperator, start, ls);
    }

    /**
     * 按一个文本（非正则表达式）分隔字符串
     * 
     * @param str
     *            字符串
     * @param plainSeperator
     *            分隔符
     * @param start
     *            开始位置
     * @param result
     *            结果，不会清空原有内容
     * @return
     * @since 1.0
     */
    public static List<String> fastSplit(final String str, final String plainSeperator, final int start,
            final List<String> result) {
        int pos = start;
        int idx = 0;
        int inc = plainSeperator.length();
        while (true) {
            idx = str.indexOf(plainSeperator, pos);
            if (idx < 0) {
                result.add(str.substring(pos, str.length()));
                break;
            }
            result.add(str.substring(pos, idx));
            pos = idx + inc;
        }
        return result;
    }

    /**
     * 按一个文本（非正则表达式）分隔字符串
     * 
     * @param str
     *            字符串
     * @param len
     *            最长长度
     * @param plainSeperator
     *            分隔符
     * @return
     * @since 1.0
     */
    public static List<String> fastSplitFixedLen(String str, String plainSeperator, int len) {
        List<String> result = new ArrayList<>();
        return fastSplitFixedLen(str, plainSeperator, len, 0, result);
    }

    /**
     * 按一个文本（非正则表达式）分隔字符串
     * 
     * @param str
     *            字符串
     * @param len
     *            最长长度
     * @param plainSeperator
     *            分隔符
     * @return
     * @since 1.0
     */
    public static List<String> fastSplitFixedLen(String str, String plainSeperator, int len, int start) {
        List<String> result = new ArrayList<>();
        return fastSplitFixedLen(str, plainSeperator, len, start, result);
    }

    /**
     * 按一个文本（非正则表达式）分隔字符串
     * 
     * @param str
     *            字符串
     * @param len
     *            最长长度
     * @param plainSeperator
     *            分隔符
     * @param result
     *            结果，不会清空原有内容
     * @return
     * @since 1.0
     */
    public static List<String> fastSplitFixedLen(String str, String plainSeperator, int len, int start,
            List<String> result) {
        int pos = start;
        int idx = 0;
        int count = 1;
        int inc = plainSeperator.length();
        while (count < len) {
            idx = str.indexOf(plainSeperator, pos);
            if (idx < 0) {
                result.add(str.substring(pos, str.length()));
                count++;
                pos = str.length();
                break;
            }
            result.add(str.substring(pos, idx));
            pos = idx + inc;
            count++;
        }
        if (idx >= 0) {
            result.add(str.substring(pos, str.length()));
        }
        return result;
    }

    /**
     * 按一个字符分隔字符串
     * 
     * @param str
     * @param ch
     * @param len
     * @return
     * @since 1.0
     */
    public static List<String> fastSplit(String str, char ch, int len) {
        List<String> ls = new ArrayList<String>(len);
        return fastSplit(str, ch, 0, ls);
    }

    /**
     * 按一个字符分隔字符串
     * 
     * @param str
     * @param ch
     * @param start
     * @param len
     * @return
     * @since 1.0
     */
    public static List<String> fastSplit(String str, char ch, int len, int start) {
        List<String> ls = new ArrayList<String>(len);
        return fastSplit(str, ch, start, ls);
    }

    /**
     * 按一个字符分隔字符串
     * 
     * @param str
     *            字符串
     * @param plainSeperator
     *            分隔符
     * @param result
     *            结果，不清空原有内容
     * @return
     * @since 1.0
     */
    public static List<String> fastSplit(String str, char ch, int start, List<String> result) {
        int pos = start;
        int idx = 0;
        while (true) {
            idx = str.indexOf(ch, pos);
            if (idx < 0) {
                result.add(str.substring(pos, str.length()));
                break;
            }
            result.add(str.substring(pos, idx));
            pos = idx + 1;
        }
        return result;
    }

    /**
     * 按一个字符分隔字符串
     * 
     * @param str
     *            字符串
     * @param len
     *            最长长度
     * @param plainSeperator
     *            分隔符
     * @return
     * @since 1.0
     */
    public static List<String> fastSplitFixedLen(String str, char ch, int len) {
        List<String> result = new ArrayList<>();
        return fastSplitFixedLen(str, ch, len, 0, result);
    }

    /**
     * 按一个字符分隔字符串
     * 
     * @param str
     *            字符串
     * @param len
     *            最长长度
     * @param plainSeperator
     *            分隔符
     * @return
     * @since 1.0
     */
    public static List<String> fastSplitFixedLen(String str, char ch, int len, int start) {
        List<String> result = new ArrayList<>();
        return fastSplitFixedLen(str, ch, len, start, result);
    }

    /**
     * 按一个字符分隔字符串
     * 
     * @param str
     *            字符串
     * @param len
     *            最长长度
     * @param plainSeperator
     *            分隔符
     * @param result
     *            结果，不清空原有内容
     * @return
     * @since 1.0
     */
    public static List<String> fastSplitFixedLen(String str, char ch, int len, int start, List<String> result) {
        int pos = start;
        int idx = 0;
        int count = 1;
        while (count < len) {
            idx = str.indexOf(ch, pos);
            if (idx < 0) {
                result.add(str.substring(pos, str.length()));
                count++;
                pos = str.length();
                break;
            }
            result.add(str.substring(pos, idx));
            pos = idx + 1;
            count++;
        }
        if (idx >= 0) {
            result.add(str.substring(pos, str.length()));
        }
        return result;
    }

    /**
     * 打印数组
     * 
     * @param array
     * @return
     */
    public static String arrayToString(int[] array) {
        SplitValueBuilder svb = new SplitValueBuilder();
        for (int i : array) {
            svb.add(i);
        }
        return svb.build();
    }

    /**
     * 打印数组
     * 
     * @param array
     * @return
     */
    public static String arrayToString(long[] array) {
        SplitValueBuilder svb = new SplitValueBuilder();
        for (long i : array) {
            svb.add(i);
        }
        return svb.build();
    }

    /**
     * 打印数组
     * 
     * @param array
     * @return
     */
    public static String arrayToString(boolean[] array) {
        SplitValueBuilder svb = new SplitValueBuilder();
        for (boolean i : array) {
            svb.add(i == true ? 1 : 0);
        }
        return svb.build();
    }

    /**
     * 打印数组
     * 
     * @param array
     * @return
     */
    public static String arrayToString(int[] array, String split) {
        SplitValueBuilder svb = new SplitValueBuilder(split);
        for (int i : array) {
            svb.add(i);
        }
        return svb.build();
    }

    /**
     * 查找链表中的元素（消除越界等异常）
     * 
     * @param list
     *            链表
     * @param index
     *            链表下标
     * @return 链表中的元素
     */
    public static String findListItem(List<String> list, int index) {
        if (index < 0) {
            return "";
        }
        try {
            return list.get(index).trim();
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * 获取字符串的MD5值
     * 
     * @param args
     */
    public static String getMd5(String str) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(str.getBytes());
            byte b[] = md.digest();

            int i;

            StringBuffer buf = new StringBuffer("");
            for (int offset = 0; offset < b.length; offset++) {
                i = b[offset];
                if (i < 0)
                    i += 256;
                if (i < 16)
                    buf.append("0");
                buf.append(Integer.toHexString(i));
            }
            return buf.toString();
        } catch (Exception e) {
            return null;
        }
    }

    public static class LengthAndStringReader {
        private int curr;
        private String str;
        private String seperator;

        public LengthAndStringReader(String str, String seperator) {
        }

        public void reset(String str, String seperator) {
            curr = 0;
            this.str = str;
            this.seperator = seperator;
        }

        public String readString() {
            int start = curr;
            int bar = str.indexOf(seperator, start);
            int size = Integer.parseInt(str.substring(start, bar));
            bar++;
            curr = bar + 1 + size;
            return str.substring(bar, bar + size);
        }

        public String remainString() {
            return curr < 0 || curr >= str.length() ? "" : str.substring(curr);
        }

        public long readLong() {
            String str = nextToken();
            return Long.parseLong(str);
        }

        public String nextToken() {
            int start = curr;
            int bar = str.indexOf('|', start);
            curr = bar + 1;
            return str.substring(start, bar);
        }

        public static void writeString(String str, StringBuilder sb, String seperator) {
            sb.append(str.length()).append(seperator).append(str);
        }

        public static void writeString(String str, SplitValueBuilder svb) {
            svb.add(str.length()).add(str);
        }
    }
}
