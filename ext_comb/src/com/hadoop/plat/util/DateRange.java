package com.hadoop.plat.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * 
 * @since 1.0
 */

public class DateRange {
    public static final String PROPERTY_FORMAT = "DateRange.format.type";
    public static String FORMAT_STRING_1 = "yyyy-MM-dd";
    public static String FORMAT_STRING_2 = "yyyyMMdd";
    public static String FORMAT_STRING_1_WITH_MONTH = "yyyy-MM/yyyy-MM-dd";
    public static String FORMAT_STRING_2_WITH_MONTH = "yyyyMM/yyyyMMdd";
    private final SimpleDateFormat FORMAT1 = new SimpleDateFormat(FORMAT_STRING_1);
    private static final SimpleDateFormat FORMAT1_WITH_MONTH = new SimpleDateFormat(FORMAT_STRING_1_WITH_MONTH);
    private final SimpleDateFormat FORMAT2 = new SimpleDateFormat(FORMAT_STRING_2);
    private static final SimpleDateFormat FORMAT2_WITH_MONTH = new SimpleDateFormat(FORMAT_STRING_2_WITH_MONTH);
    private Date dateStart;
    private Calendar ending;
    private SimpleDateFormat dateFormat;

    public DateRange(String start, String end, boolean seperatedByMonth, Configuration conf) {
        if (StringUtils.isBlank(start) || StringUtils.isBlank(end)) {
            throw new IllegalArgumentException("Date (start or end) can not be blank.");
        }
        Date dateEnd = null;
        try {
            dateStart = FORMAT1.parse(start);
            dateEnd = FORMAT1.parse(end);
            dateFormat = seperatedByMonth ? FORMAT1_WITH_MONTH : FORMAT1;
            conf.setInt(PROPERTY_FORMAT, 1);
        } catch (ParseException e) {}
        if (dateFormat == null) {
            try {
                dateStart = FORMAT2.parse(start);
                dateEnd = FORMAT2.parse(end);
                dateFormat = seperatedByMonth ? FORMAT2_WITH_MONTH : FORMAT2;
                conf.setInt(PROPERTY_FORMAT, 2);
            } catch (ParseException e) {}
        }
        if (dateFormat == null) {
            throw new IllegalArgumentException("Can't resolve time format of string " + start + " and " + end
                    + "\nSupported formats are " + FORMAT_STRING_1 + "," + FORMAT_STRING_2);
        }
        if (dateStart.compareTo(dateEnd) > 0) {
            throw new IllegalArgumentException("Starting Date can not greater than Ending date.");
        }
        ending = Calendar.getInstance();
        ending.setTime(dateEnd);
    }

    public static String getFormat(String date, Configuration conf) {
        int format = conf.getInt(PROPERTY_FORMAT, Integer.MIN_VALUE);
        if (format == Integer.MIN_VALUE) {
            new DateRange(date, date, false, conf);
            format = conf.getInt(PROPERTY_FORMAT, Integer.MIN_VALUE);
        }
        switch (conf.getInt(PROPERTY_FORMAT, 1)) {
            case 2:
                return FORMAT_STRING_2;
            case 1:
            default:
                return FORMAT_STRING_1;
        }
    }

    public List<String> getDateStrings() {
        List<String> result = new ArrayList<>();
        Calendar current = Calendar.getInstance();
        current.setTime(dateStart);
        while (!current.after(ending)) {
            Date date = current.getTime();
            result.add(dateFormat.format(date));
            current.add(Calendar.DATE, 1);
        }
        return result;
    }
}
