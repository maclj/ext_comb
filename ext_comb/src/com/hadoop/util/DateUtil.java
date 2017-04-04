package com.hadoop.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期工具类。
 * 
 * 
 *
 */
public class DateUtil {

    public static final String FORMAT_DATE = "yyyy-MM-dd HH:mm:ss";
    
    /**
     * 返回当前格式化的当前时间戳
     * @return
     */
    public static final String currentDate() {
        long cur = System.currentTimeMillis();
        return new SimpleDateFormat(FORMAT_DATE).format(new Date(cur));
    }
}
