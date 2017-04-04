package com.hadoop.plat.trie;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于DoubleArrayTrie的TripMap实现。<br>
 * 
 * 使用方法：<br>
 * 
 * 1、构造，通过构造函数或者Builder（put或者InputStream）<br>
 * 2、查找，get（精确）和prefixSearch（前缀匹配）<br>
 * 
 * 不支持构造后重新修改数据。<br>
 * 
 * 
 *
 * @param <T>
 */
public class TrieMap<T> {
    
    /** 构建trie的数据，必须按照字符序排列好  */
    private List<String> trieValueList = new ArrayList<String>();
    
    /** 实际获取的数据，顺序与trieValueList保持一致   */
    private List<T> dataList = new ArrayList<T>();
    
    private DoubleArrayTrie trie = new DoubleArrayTrie();
    
    /**
     * 构造
     * @param trieValueList
     * @param dataList
     */
    public TrieMap(List<String> trieValueList, List<T> dataList) {
        this.trieValueList = trieValueList;
        this.dataList = dataList;
        trie.build(this.trieValueList);
        this.trieValueList = null;
    }
    
    TrieMap() {
    }
    
    public T get(String key) {
        int index = getIndex(key);
        if(index < 0 || index > dataList.size()) {
            return null;
        }
        return this.dataList.get(index);
    }
    
    public int getIndex(String key) {
        int index = this.trie.exactMatchSearch(key);
        return index;
    }
    
    public List<T> prefixSearch(String prefix) {
        List<Integer> indexes = prefixSearchIndex(prefix);
        List<T> value = new ArrayList<T>();
        for(Integer index : indexes) {
            if(index < 0 || index > dataList.size()) {
                continue;
            }
            value.add(this.dataList.get(index));
        }
        return value;
    }
    
    public List<Integer> prefixSearchIndex(String prefix) {
        List<Integer> indexes = this.trie.commonPrefixSearch(prefix);
        return indexes;
    }
    
    /**
     * TrieMap构建器，必须按照字符序挨个加入key和value
     * 
     *
     * @param <T>
     */
    public static class Builder<T> {
        
        private TrieMap<T> map = new TrieMap<T>();
        
        public Builder<T> put(String key, T value) {
            if(map.trieValueList == null) {
                // 只能初始化一次
                throw new IllegalStateException("TrieMap has been initialized.");
            }
            map.trieValueList.add(key);
            map.dataList.add(value);
            return this;
        }
        
        public Builder<T> open(InputStream is) throws Exception {
            map.trie.open(is);
            return this;
        }
        
        public Builder<T> add(List<T> dataList) throws Exception {
            map.dataList = dataList;
            return this;
        }
        
        public TrieMap<T> build() {
            map.trie.build(map.trieValueList);
            map.trieValueList = null;
            return map;
        }
    }
    
    
}
