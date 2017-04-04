package com.hadoop.plat.trie;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;

import com.hadoop.plat.util.StringUtil;

/**
 * 仿Trie实现的域名检索树<br/>
 * 如域名m.3c.taobao.com|淘宝|可分段、移动域名，将其按"."拆分，最后两个（com.taobao）作为第一路径，然后依次遍历/存放，
 * 在最后一个节点(com.taobao.2c.m) 保存DomainPattern。<br/>
 * <strong>线程不安全</strong>
 * @see DomainPattern
 * 
 */

public final class DomainTrie<T extends DomainPattern> implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /** 根节点 */
    private final State<T> rootState = new State<T>(null);
    /** 用来合并结果 */
    private final ResultCombiner<T> resultCombiner;
    /** 用来给状态上添加pattern */
    private final PatternCombiner<T> patternCombiner;
    /** 用来在检索时遍历要匹配的域名 */
    private transient final Iterator<T> iter = new Iterator<>();

    /**
     * 用默认combiner合并结果
     */
    public DomainTrie() {
        this(new ResultCombiner<T>(), new PatternCombiner<T>());
    }

    /**
     * 
     * @param combiner
     */
    public DomainTrie(final ResultCombiner<T> resultCombiner) {
        this(resultCombiner, new PatternCombiner<T>());
    }

    /**
     * 
     * @param combiner
     */
    public DomainTrie(final PatternCombiner<T> patternCombiner) {
        this(new ResultCombiner<T>(), patternCombiner);
    }

    /**
     * 
     * @param combiner
     */
    public DomainTrie(final ResultCombiner<T> resultCombiner, final PatternCombiner<T> patternCombiner) {
        this.resultCombiner = resultCombiner;
        this.patternCombiner = patternCombiner;
    }

    /**
     * 
     * @param pattern
     * @since 1.0
     */
    public final void addDomain(final T pattern) {
        addDomain(pattern, patternCombiner);
    }

    /**
     * @param pattern
     * @since 1.0
     */
    public final void addDomain(final T pattern, final PatternCombiner<T> patternCombiner) {
        String domain = pattern.getRealDomain();
        iter.reset(rootState, domain);
        addAllState(iter);
        patternCombiner.addPattern(iter.get(), pattern);
    }

    /**
     * 
     * @param iter
     */
    private void addAllState(final Iterator<T> iter) {
        while (iter.hasNextSplitDomain()) {
            iter.createNext();
            iter.next();
        }
    }

    /**
     * 
     * @param domain
     * @return
     * @since 1.0
     */
    public final Collection<T> findDomainPatterns(final String domain) {
        return findDomainPatterns(domain, new LinkedHashMap<String, T>());
    }

    /**
     * 
     * @param domain
     * @return
     * @since 1.0
     */
    public final Collection<T> findDomainPatterns(final String domain, final Map<String, T> defaultMap) {
        Map<String, T> result = defaultMap;
        boolean success = iter.reset(rootState, domain);
        if (!success) {
            return null;
        }
        while (iter.hasNext()) {
            State<T> state = iter.next();
            Map<String, T> patterns = state.getPatternMap();
            if (patterns != null) {
                if (!resultCombiner.combine(domain, result, patterns, iter))
                    return null;
            }
        }
        return result.values();
    }

    /**
     * 
     * @param domain
     * @return
     * @since 1.0
     */
    public final Map<String, Collection<T>> findDomainAllPatterns(final String domain, final String realdomain,
                                                                  Map<String, Collection<T>> result) {
        if (result == null) {
            result = new HashMap<String, Collection<T>>();
        }
        boolean success = iter.reset(rootState, realdomain);
        if (!success) {
            return null;
        }
        while (iter.hasNext()) {
            State<T> state = iter.next();
            Map<String, T> patterns = state.getPatternMap();
            if (patterns != null) {
                if (!resultCombiner.combineAll(domain, result, patterns, iter))
                    return null;
            }
        }
        return result;
    }

    /**
     * 用来遍历所有状态
     * 
     * @since 1.0
     */
    public static final class Iterator<T extends DomainPattern> {
        State<T> currentState;
        int nextIndex;
        List<String> splitDomains;

        public Iterator() {}

        private boolean reset(final State<T> root, final String domain) {
            currentState = root;
            splitDomains = StringUtil.fastSplit(domain, '.', 4);
            int len = splitDomains.size();
            if (len < 2) {
                return false;
            }
            if (splitDomains.get(len - 1).length() == 0) {
                return false;
            }
            splitDomains.set(len - 2, splitDomains.get(len - 1) + "." + splitDomains.get(len - 2));
            nextIndex = len - 2;
            return true;
        }

        /**
         * 
         * @return
         * @since 1.0
         */
        public State<T> get() {
            return currentState;
        }

        /**
         * 在调用前需确认已经存在下一个节点，hasNext返回true，或createNext
         * 
         * @return
         * @since 1.0
         */
        private final State<T> next() {
            currentState = currentState.getChildren(splitDomains.get(nextIndex--));
            return currentState;
        }

        public final boolean hasNext() {
            return currentState != null && nextIndex >= 0
                    && currentState.getChildren(splitDomains.get(nextIndex)) != null;
        }

        /** 添加下一个节点，如果下一个节点已经存在，直接返回下一个节点 */
        private final State<T> createNext() {
            State<T> nextState = currentState.getChildren(splitDomains.get(nextIndex));
            if (nextState == null) {
                nextState = currentState.addChild(splitDomains.get(nextIndex));
            }
            return nextState;
        }

        /**
         * 当前要检索的域名分成的Token，是否已经全部遍历.
         * 用于构建树。
         * 
         * @return
         * @since 1.0
         */
        public final boolean hasNextSplitDomain() {
            return nextIndex >= 0;
        }
    }

    public String toString() {
        return "DomainTrie:{rootState:" + rootState + "}";
    }

    public static class ResultCombiner<T extends DomainPattern> implements Serializable {
        /**  */
        private static final long serialVersionUID = 1L;

        public boolean combine(String domain,
                               final Map<String, T> result,
                               final Map<String, T> patterns,
                               final Iterator<T> iter) {
            for (T pattern : patterns.values()) {
                // 如果是否精确匹配
                if (pattern.isEqualMatchType() && iter.hasNextSplitDomain()) {
                    continue;
                }
                // 如果遇到黑名单，返回null
                if (Characteristics.droped(pattern.getCharacteristic())) {
                    return false;
                }
                result.put(pattern.getName(), pattern);
            }
            return true;
        }

        /**
         * 
         * @param result
         * @param patterns
         * @param iter
         * @return
         * @since 1.0
         */
        public boolean combineAll(String domain,
                                  Map<String, Collection<T>> result,
                                  Map<String, T> patterns,
                                  Iterator<T> iter) {
            throw new NotImplementedException();
        }
    }

    /**
     * 
     * 
     * @since 1.0
     */

    public static class PatternCombiner<T extends DomainPattern> implements Serializable {
        /**  */
        private static final long serialVersionUID = 1L;

        public void addPattern(State<T> state, T pattern) {
            state.addPattern(pattern);
        }

        protected Map<String, T> getPatternMap(State<T> state) {
            return state.getPatternMap();
        }

        /**
         * 
         * @param state
         * @since 1.0
         */
        protected void clearPatterns(State<T> state) {
            state.clearPatterns();
        }

        /**
         * 
         * @param state
         * @return
         * @since 1.0
         */
        protected Map<String, State<T>> getStateChildren(State<T> state) {
            return state.getChildren(false);
        }
    }
}
