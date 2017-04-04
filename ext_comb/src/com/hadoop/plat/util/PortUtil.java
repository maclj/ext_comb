package com.hadoop.plat.util;

import org.apache.commons.lang.StringUtils;

public class PortUtil {
	
	public static final int NO_PORT = -1;

	public static boolean isValidPort(String port){
		if (StringUtils.isBlank(port)){
			return false;
		}
		
		int po;
		try {
			po = Integer.valueOf(port).intValue();
		} catch (Exception e){
			return false;
		}
		
		return isValidPort(po);
	}
	
	public static boolean isValidPort(int port){
		if (port < 0 || port > 65535){
			return false;
		}
		return true;
	}
	
	public static int parse(String port){
		if (!isValidPort(port)){
			return NO_PORT;
		}
		return Integer.valueOf(port).intValue();
	}
}
