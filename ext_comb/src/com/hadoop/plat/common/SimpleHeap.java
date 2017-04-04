package com.hadoop.plat.common;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.commons.lang.ArrayUtils;

public class SimpleHeap<T extends Comparable<? super T>> implements Heap<T> {

	// 存储实际数据
	private TreeSet<T> data;

	// 堆空间大小 limit=0则全排
	private int limit;

	// 是否为小顶堆
	private boolean isMinTop;

	/**
	 * 缺省为范围100的大顶堆。
	 */
	public SimpleHeap() {
		this(false, 100);
	}

	public SimpleHeap(boolean isMinTop, int limit) {
		this.limit = limit;
		this.isMinTop = isMinTop;
		this.data = new TreeSet<>();
	}

	/**
	 * 指定排序比较。普通排序容器用
	 * 
	 * @param isMinTop
	 * @param limit
	 * @param comparator
	 */
	public SimpleHeap(boolean isMinTop, int limit, Comparator<T> comparator) {
		this.limit = limit;
		this.isMinTop = isMinTop;
		this.data = new TreeSet<T>(comparator);
	}

	/**
	 * 指定排序比较。
	 * 
	 * @param comparator
	 */
	public SimpleHeap(Comparator<T> comparator) {
		this.data = new TreeSet<T>(comparator);
	}

	/**
	 * 增加一个元素
	 * 
	 * @param t
	 */
	public void add(T t) {

		if (t == null) {
			throw new NullPointerException("input is null.");
		}
		this.data.add(t);

		if (limit == 0) {
			// limit = 0 数据全部保留
			return;
		}

		if(this.data.size() > limit) {
			T cmp = isMinTop ? this.data.last() : this.data.first();
			this.data.remove(cmp);
		}

	}

	/**
	 * 返回元素数组。
	 * 
	 * @param param
	 * @param isAscending
	 * @return
	 */
	public T[] toList(T[] param, boolean isAscending) {
		T[] rs = this.data.toArray(param);
		if (isAscending) {
			return rs;
		}
		ArrayUtils.reverse(rs);
		return rs;
	}

	/**
	 * 返回元素迭代器，注意，这个不是有序的结果。
	 * 
	 * @param isAscending
	 * @return
	 */
	public Iterator<T> iterator() {
		return this.data.iterator();
	}

	public boolean isEmpty() {
		return this.data.isEmpty();
	}

	public int getLimit() {
		return this.limit;
	}

	public boolean isMinTop() {
		return this.isMinTop;
	}

	@Override
	public String toString() {
		return "Heap [data=" + Arrays.toString(this.data.toArray())
				+ ", limit=" + limit + ", isMinTop=" + isMinTop + "]";
	}

}
