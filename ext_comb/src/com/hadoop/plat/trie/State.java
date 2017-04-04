package com.hadoop.plat.trie;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * trie树中的一个节点
 * @since 1.0
 */

public class State<T extends DomainPattern> implements Serializable{
    
    private static final long serialVersionUID = 1L;
    /** 节点深度 */
    private final int depth;
    /** 节点的孩子 */
    private Map<String, State<T>> children;
    /** 该节点符合的所有域名 */
    private Map<String, T> patterns;
    /** 父节点 */
    private State<T> parent;

    public State(final int depth, final T pattern, final State<T> parent) {
        this.depth = depth;
        if (pattern != null) {
            addPattern(pattern);
        }
        this.parent = parent;
    }

    State(final State<T> parent) {
        this(0, null, parent);
    }

    /** 添加一个孩子，并设置其匹配的平台 */
    public final State<T> addCihld(final String transition, final T pattern) {
        State<T> nextState = getChildren().get(transition);
        if (nextState == null) {
            nextState = new State<T>(this.depth + 1, pattern, this);
            children.put(transition, nextState);
        }
        if (pattern != null){
            nextState.addPattern(pattern);
        }
        return nextState;
    }

    /**
     * 给该节点添加匹配的平台
     * @param pattern2
     * @since  1.0
     */
    public void addPattern(final T pattern) {
        if (patterns == null) {
            patterns = new HashMap<>();
            this.patterns.put(pattern.getName(), pattern);
        } else {
            String patternName = pattern.getName();
            DomainPattern oldPattern = patterns.get(patternName);
            if (oldPattern != null) {
                oldPattern.newCharacteristic(pattern.getCharacteristic());
            } else {
                patterns.put(patternName, pattern);
            }
        }
    }

    /**
     * 添加一个孩子节点
     * @param transition
     * @return
     * @since  1.0
     */
    public final State<T> addChild(final String transition) {
        return addCihld(transition, null);
    }

    final Map<String, State<T>> getChildren() {
        return getChildren(true);
    }

    final Map<String, State<T>> getChildren(boolean create) {
        if (children == null && create) {
            children = new HashMap<>();
        }
        return children;
    }

    /**
     * 获取孩子节点
     * @param transition
     * @return
     * @since  1.0
     */
    public final State<T> getChildren(final String transition) {
        return children == null ? null : children.get(transition);
    }

    /**
     * @return the pattern
     */
    public final Collection<T> getPatterns() {
        return patterns == null ? null : patterns.values();
    }
    
    /**
     * @return the pattern
     */
    void clearPatterns() {
        patterns = null;
    }
    
    /**
     * 判断该节点是否含有匹配的平台
     * @return
     * @since  1.0
     */
    public final boolean hasPattern() {
        return patterns != null;
    }
    
    public final String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("State:{depth:" + depth + ",patterns:" + patterns + ",children:" + children + "}");
        return sb.toString();
    }

    /**
     * @return
     * @since  1.0
     */
    Map<String, T> getPatternMap() {
        if (patterns == null)
            patterns = new HashMap<>();
        return patterns;
    }

    public State<T> getParent() {
        return parent;
    }
}
