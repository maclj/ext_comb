package com.hadoop.plat.util;

/**
 * 使用指定分隔符生成字符串.
 * 
 *
 */
public class SplitValueBuilder {

	private String split = "|";
	private StringBuilder sb = new StringBuilder();

	public SplitValueBuilder() {
	}

	public SplitValueBuilder(String split) {
		this.split = split;
	}

	public SplitValueBuilder add(String value) {
		this.sb.append(value).append(split);
		return this;
	}
	
	public SplitValueBuilder add(int value) {
		this.sb.append(String.valueOf(value)).append(split);
		return this;
	}
	
	public SplitValueBuilder add(long value) {
		this.sb.append(String.valueOf(value)).append(split);
		return this;
	}
	
	public SplitValueBuilder add(Object value) {
		this.sb.append(String.valueOf(value)).append(split);
		return this;
	}

	public String buildWithLast() {
		return this.sb.toString();
	}
	
	public void clear() {
	    this.sb.delete(0, this.sb.length());
	}

	public String build() {
		return toString();
	}

	public String toString() {
		int index = this.sb.lastIndexOf(split);
		if (index > 0 && (index + split.length()) == this.sb.length()) {
			return this.sb.substring(0, index);
		}
		return this.sb.toString();
	}

	public static void main(String[] args) {
		SplitValueBuilder svb = new SplitValueBuilder(",");
		svb.add("abc").add("123");
		System.out.println(svb.build());
		System.out.println(svb.buildWithLast());
		System.out.println(svb);
		svb.clear();
		System.out.println(svb);
		svb.add("123").add("abc");
		System.out.println(svb);
	}
}
