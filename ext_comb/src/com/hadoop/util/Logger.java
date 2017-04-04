package com.hadoop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.hadoop.entry.Jobs;

/**
 * log util
 * 
 *
 */
public class Logger {

    private static Log log = LogFactory.getLog(Logger.class);
    
    private static Configuration CONF;
    
    public static void init(Configuration conf) {
        CONF = conf;
    }

    public static void print(String value) {
        if (Jobs.isEnvSysoutEnabled(CONF)) {
            if (value != null) {
                System.out.println(value);
            }
        }
    }

    public static void info(String value) {
        
        if (Jobs.isEnvLogEnabled(CONF)) {
            log.info(value);
            return;
        }
        if (Jobs.isEnvSysoutEnabled(CONF)) {
            if (value != null) {
                System.out.println(value);
            }
        }
    }

    public static void debug(String value) {
        if (Jobs.isEnvLogEnabled(CONF)) {
            log.debug(value);
            return;
        }
        if (Jobs.isEnvSysoutEnabled(CONF)) {
            if (value != null) {
                System.out.println(value);
            }
        }
    }

    public static void debug(String value, Throwable t) {
        if (Jobs.isEnvLogEnabled(CONF)) {
            log.debug(value, t);
            return;
        }
        if (Jobs.isEnvSysoutEnabled(CONF)) {
            if (value != null) {
                System.out.println(value);
                if (t != null) {
                    t.printStackTrace(System.out);
                }
            }
        }
    }

    public static void warn(String value) {
        if (Jobs.isEnvLogEnabled(CONF)) {
            log.warn(value);
            return;
        }
        if (Jobs.isEnvSysoutEnabled(CONF)) {
            if (value != null) {
                System.out.println(value);
            }
        }
        
    }

    public static void warn(String value, Throwable t) {
        
        if (Jobs.isEnvLogEnabled(CONF)) {
            log.warn(value, t);
            return;
        }
        
        if (Jobs.isEnvSysoutEnabled(CONF)) {
            if (value != null) {
                System.out.println(value);
            }
            if (t != null) {
                t.printStackTrace();
            }
        }
        
    }
}
