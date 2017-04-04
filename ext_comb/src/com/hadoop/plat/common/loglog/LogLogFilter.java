package com.hadoop.plat.common.loglog;

/**
 * LogLog算法
 * 实现数据集中不重复数据的统计
 * 
 *
 */
public class LogLogFilter {

	private HyperLogLogPlus plus;
	
	/**
	 * 构造方法
	 * @param p 用于构建桶的位数
	 */
	public LogLogFilter(int p){
		plus = new HyperLogLogPlus(p);
	}
	
	/**
	 * 增加数据
	 * 注意： 必须实现toString()方法
	 * @param o 数据
	 */
	public void add(Object o){
		plus.offer(o);
	}
	
	/**
	 * 获取不重复数据的统计数
	 * @return
	 */
	public long getCardinality(){
		return plus.cardinality();
	}
	
	/**
	 * 获取判断不重复数据所用的字节数
	 * @return
	 */
	public int getSize(){
		return plus.sizeof();
	}
}
