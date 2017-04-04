package com.hadoop.util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;

/**
 * MD5工具
 * 
 *
 */
public class MD5Util {

    private static String algorithm="MD5";
    
    /**
     * 生成MD5
     * @param data
     * @return
     */
    public static byte[] md5(byte[] data) {
        if(data == null || data.length == 0){
            return null;
        }
        try {
            MessageDigest md = MessageDigest.getInstance(algorithm);
            md.update(data);
            return md.digest();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    
    /**
     * 字符串结果
     * @param data
     * @return
     */
    public static String getMD5(String data) {
        if(data == null || data.length() == 0){
            return null;
        }
        byte[] input = null;
        try {
            input = data.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
        byte[] value = md5(input);
        String output = convertToHexString(value);
        return output;
    }
    
    /**
     * 大写结果
     * @param data
     * @return
     */
    public static String getMD5Upper(String data) {
        String output = getMD5(data);
        if(output == null) {
            return null;
        }
        return output.toUpperCase();
    }
    
    /**
     * 直接连接的版本
     * @param data
     * @param salt
     * @return
     */
    public static String getMD5Salt(String data, String salt) {
        return getMD5(data + salt);
    }
    
    public static String getMD5UpperSalt(String data, String salt) {
        return getMD5Upper(data + salt);
    }
    
    /**
     * to hex.
     * @param data
     * @return
     */
    public static String convertToHexString(byte data[]) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < data.length; i++) {
            String hex = Integer.toHexString(0xff & data[i]);
            if (hex.length() == 1) {
                sb.append('0');
            }
            sb.append(hex);
        }
        return sb.toString();
    }
    
    public static void main(String[] args) {
        String data = "abc";
        System.out.println(getMD5(data));
//        System.out.println(getMD5Old(data));
    }
}
