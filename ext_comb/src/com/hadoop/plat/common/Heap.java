package com.hadoop.plat.common;

import java.util.Iterator;

public interface Heap<T> {

	void add(T t);
	
	T[] toList(T[] param, boolean isAscending);
	
	Iterator<T> iterator();
}
