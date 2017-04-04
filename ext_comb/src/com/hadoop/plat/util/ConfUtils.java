package com.hadoop.plat.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * 
 */

public class ConfUtils {
    /** 注释字符，如果一行以两个#开头，认为该行为注释 */
    public static final char COMMENT_LINE_PREFIX = '#';
    /** 默认字符集 */
    public static final Charset DEFAULT_CHARSET = Charset.forName("utf-8");
    /** 日志 */
    private static final Logger LOG = Logger.getLogger(ConfUtils.class);

    /**
     * 从文件读取文件，并使用comsumer处理
     * @param fileName 文件名
     * @param consumer 处理文件的一行
     * @param optional 如果为false且文件不存在，抛出IllegalArgumentException;如果true且文件不存在，打印一条日志。
     * @param skipHead 跳过前n行
     * @param skipTail 跳过后n行
     * @param countAnyTime 如果为false，则consumer的行号为有效行的行号（不计算head、空行和注释航）；否则为文件中的行号。
     * @throws IOException
     */
    public static void loadFile(String fileName,
                                ConfLineConsumer consumer,
                                boolean optional,
                                int skipHead,
                                int skipTail)
        throws IOException {
        File file = new File(fileName);
        if (!file.exists()) {
            if (optional) {
                LOG.info("File " + fileName + " not found.");
            } else {
                throw new IllegalArgumentException("File " + fileName + " not found.");
            }
        }
        try (BufferedReader br =
                new BufferedReader(new InputStreamReader(new FileInputStream(file), DEFAULT_CHARSET))) {
            for (int i = 0; i < skipHead; i++) {
                if (null == br.readLine()) {
                    return;
                }
            }
            if (skipTail <= 0) {
                noTail(br, consumer, skipHead);
            } else {
                withTail(br, consumer, skipHead, skipTail);
            }
        }
    }

    /**
     * 读取skipTail=0的配置文件
     * @param br
     * @param comsumer
     * @param row
     * @throws IOException
     */
    private static void noTail(final BufferedReader br, final ConfLineConsumer comsumer, int row) throws IOException {
        for (String line = br.readLine(); line != null; line = br.readLine()) {
            if (StringUtils.isBlank(line)) {
                continue;
            }
            if (isComment(line)) {
                if (!comsumer.acceptComment(line, row++)) {
                    return;
                }
            } else {
                if (!comsumer.accept(line, row++)) {
                    return;
                }
            }
        }
    }

    /**
     * 读取skipTail > 0的配置
     * @param br
     * @param comsumer
     * @param row
     * @param tails
     * @throws IOException
     */
    private static void withTail(final BufferedReader br, final ConfLineConsumer comsumer, int row, int tails)
        throws IOException {
        //TODO implement
        throw new RuntimeException("Not implemented.");
    }

    /**
     * 判断一个行是否为注释
     * @param line
     * @return
     */
    public static boolean isComment(final String line) {
        return line.charAt(0) == COMMENT_LINE_PREFIX && line.charAt(1) == COMMENT_LINE_PREFIX;
    }

    /**
     * 处理配置文件内容
     * @since 1.0
     */
    public static abstract class ConfLineConsumer {
        /**
         * 每读到一个有效行时，调用该方法
         * @param line 行内容
         * @param i 行号
         * @throws IOException
         */
        protected boolean accept(String line, int i) throws IOException {
            return true;
        }

        /**
         * 每读到一行注释时，调用该方法
         * @param line 行内容
         * @param i 行号
         * @throws IOException
         */
        protected boolean acceptComment(String line, int i) throws IOException {
            return true;
        }
    }

    /**
     * 以key、value的方式读入map，可以设置分隔符，如果一行中没有分隔符，改行将作为key
     */
    public static class MapConsumer extends ConfLineConsumer {
        private final Map<String, String> result;
        private final char seperator;

        public MapConsumer(final Map<String, String> map, final char seperator) {
            this.result = map;
            this.seperator = seperator;
        }

        @Override
        public boolean accept(String line, int i) throws IOException {
            if (StringUtils.isBlank(line)) {
                return true;
            }
            int index = line.indexOf(seperator);
            if (index < 0) {
                result.put(line, null);
            } else {
                String key = line.substring(0, index);
                String value = line.substring(index + 1);
                result.put(key, value);
            }
            return true;
        }
    }
}
