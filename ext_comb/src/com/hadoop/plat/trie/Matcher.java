package com.hadoop.plat.trie;

/**
 * 用于匹配一个域名和配置的域名是否匹配
 * 
 * 
 * @since 1.0
 */

public abstract class Matcher {

	/**
	 * @param source
	 * @param pattern
	 * @return
	 */
	public abstract boolean matchs(String source, String pattern);
	
	/** 匹配标识：Equals  */
	public static final String MATCH_EQUALS = "1";
	
	/** 匹配标识：Endwith  */
	public static final String MATCH_ENDWITH = "0";
	
	/** Mathc实例 */
	private static Matcher equalsMatcher = new EqualMatcher();
	
	/** Mathc实例 */
	private static Matcher endwithMatcher = new EndWithMatcher();

	/**
	 * 获取Matcher实例。
	 * @param type
	 * @return
	 */
	public static Matcher get(String type) {
		boolean isEndwith = MATCH_ENDWITH.equals(type);
		return isEndwith ? endwithMatcher : equalsMatcher;
	}

	private static class EqualMatcher extends Matcher {
		@Override
		public boolean matchs(String source, String pattern) {
			return source.equals(pattern);
		}
	}

	private static class EndWithMatcher extends Matcher {
		@Override
		public boolean matchs(String source, String pattern) {
			return source.endsWith(pattern);
		}
	}
}
