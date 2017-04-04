package com.hadoop.util.concurrent;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 并发Set。
 *
 * @param <E>
 * 
 * 
 */
public class ConcurrentSet<E> implements Set<E>, Cloneable, Serializable {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = -6165624530317243929L;

	/**
	 * 利用ConcurrentHashMap实现。
	 */
	private ConcurrentHashMap<E, Boolean> map = new ConcurrentHashMap<E, Boolean>();

	/**
	   *
	   */
	public ConcurrentSet() {
		this.map = new ConcurrentHashMap<E, Boolean>();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.util.Set#add(E)
	 */
	public boolean add(E item) {
		return map.put(item, Boolean.TRUE) == null;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.util.Set#addAll(java.util.Collection)
	 */
	public boolean addAll(Collection<? extends E> items) {
		boolean changed = false;
		for (E item : items) {
			/* update flag determining whether set has changed or not */
			changed = add(item) || changed;
		}
		return changed;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.util.Set#clear()
	 */
	public void clear() {
		map.clear();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.util.Set#contains(java.lang.Object)
	 */
	public boolean contains(Object item) {
		return map.containsKey(item);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.util.Set#containsAll(java.util.Collection)
	 */
	public boolean containsAll(Collection<?> items) {
		return map.keySet().containsAll(items);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.util.Set#isEmpty()
	 */
	public boolean isEmpty() {
		return map.isEmpty();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.util.Set#iterator()
	 */
	public Iterator<E> iterator() {
		return map.keySet().iterator();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.util.Set#remove(java.lang.Object)
	 */
	public boolean remove(Object item) {
		/* we double up argument as both key and value */
		return map.remove(item, Boolean.TRUE);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.util.Set#removeAll(java.util.Collection)
	 */
	public boolean removeAll(Collection<?> items) {
		return map.keySet().removeAll(items);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.util.Set#retainAll(java.util.Collection)
	 */
	public boolean retainAll(Collection<?> items) {
		return map.keySet().retainAll(items);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.util.Set#size()
	 */
	public int size() {
		return map.size();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.util.Set#toArray()
	 */
	public Object[] toArray() {
		return map.keySet().toArray();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.util.Set#toArray(T[])
	 */
	public <T> T[] toArray(T[] array) {
		return map.keySet().toArray(array);
	}

    /**
     * Returns a shallow copy of this <tt>ConcurrentSet</tt> instance: the elements
     * themselves are not cloned.
     *
     * @return a shallow copy of this set
     */
    public Object clone() {
        ConcurrentSet<E> newSet = new ConcurrentSet<E>();
		newSet.map = new ConcurrentHashMap<E, Boolean>(map);
		return newSet;
    }

//	public static void main(String[] args) {
//		ConcurrentSet<String> set = new ConcurrentSet<String>();
//	}
}
