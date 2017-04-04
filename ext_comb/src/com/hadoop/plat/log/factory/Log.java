package com.hadoop.plat.log.factory;

import java.io.Serializable;

/**
 * 通用日志结构，如nginx、crawler
 *
 */
public class Log implements Serializable {

    private static final long serialVersionUID = -5287990590199924570L;

    public Log() {
        throw new IllegalStateException("Not Implemented");
    }
    
    public int getType() {
        throw new IllegalStateException("Not Implemented");
    }
}
