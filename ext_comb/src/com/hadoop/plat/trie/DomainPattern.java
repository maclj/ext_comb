package com.hadoop.plat.trie;

import java.io.Serializable;
import java.util.Objects;

import org.apache.commons.lang.StringUtils;


public class DomainPattern implements Serializable {

    private static final long serialVersionUID = 8847491922153145143L;

    private static final String SP = "\\|";

    private String realDomain;

    private long characteristic;

    private String matchType;

    private boolean equalMatchType;

    private String name;
    
    private String category;

    public DomainPattern() {}

    /**
     * 构造配置
     * @param name
     * @param domain
     * @param category
     * @param matchType
     * @param isWhite
     */
    protected DomainPattern(String name, String domain, String category, String matchType, boolean isWhite) {
        super();
        this.name = name;
        this.realDomain = domain;
        this.category = category;
        if (isWhite) {
            newCharacteristic(Characteristics.CHARACTERISTIC_VALID_SEGMENTABLE); // 设置有效标识
        } else {
            newCharacteristic(Characteristics.CHARACTERISTIC_DROPED);// 设置删除标识
        }
        setMatchType(matchType);
    }

    // ======================  缺省白名单格式的解析实现 ===============================
    
    /** 白名单中真实域名索引 */
    public static final int INDEX_WHITELIST_REALDOMAIN = 0;
    /** 白名单中域名索引 */
    public static final int INDEX_WHITELIST_DOMAIN = 1;
    /** 白名单中分类索引 */
    public static final int INDEX_WHITELIST_CATEGORY = 2;
    /** 白名单中匹配方式索引 */
    public static final int INDEX_WHITELIST_MATCH = 3;
    /** 白名单一条记录长度 */
    public static final int LENGTH_WHITELIST = 4;

    /**
     * @param log
     * @return
     */
    public static DomainPattern parseWhiteList(String log) {
        String[] tokens = log.split(SP);
        if (tokens.length < LENGTH_WHITELIST) {
            return null;
        }
        DomainPattern dp = new DomainPattern();
        dp.name = tokens[INDEX_WHITELIST_DOMAIN].trim(); // 网站名称
        dp.realDomain = tokens[INDEX_WHITELIST_REALDOMAIN].trim().toLowerCase(); // 真实的HOST信息
        dp.matchType = tokens[INDEX_WHITELIST_MATCH].trim(); // 匹配类型
        dp.equalMatchType = Matcher.MATCH_EQUALS.equals(dp.matchType); // 匹配类型  
        dp.newCharacteristic(Characteristics.CHARACTERISTIC_VALID_SEGMENTABLE); // 设置有效标识
        dp.setCategory(tokens[INDEX_WHITELIST_CATEGORY]); // 网站分类
        return dp;
    }
    
    // ======================  缺省黑名单格式的解析实现 ===============================

    /** 黑名单中真实域名索引 */
    public static final int INDEX_BLACKLIST_REALDOMAIN = 0;
    /** 黑名单中匹配方式索引 */
    public static final int INDEX_BLACKLIST_MATCH = 1;
    /** 黑名单一条记录长度 */
    public static final int LENGTH_BLACKLIST = 2;

    /**
     * @param log
     * @return
     */
    public static DomainPattern parseBlackList(String line) {
        if (StringUtils.isEmpty(line))
            return null;
        String[] tokens = line.split(SP);
        if (tokens.length < 2) {
            return null;
        }
        DomainPattern  pattern = new DomainPattern();
        pattern.realDomain = tokens[INDEX_BLACKLIST_REALDOMAIN].toLowerCase();
        pattern.matchType = tokens[INDEX_BLACKLIST_MATCH].trim();
        pattern.newCharacteristic(Characteristics.CHARACTERISTIC_DROPED);// 设置删除标识
        pattern.name = "blackList"; // 黑名单没有提供域名标识，采用固定占位符，之前采用NULL标识
        pattern.equalMatchType = Matcher.MATCH_EQUALS.equals(pattern.matchType);
        
        return  pattern;
    }
    
    /**
     * @param characteristic
     * @since 1.0
     */
    public final void newCharacteristic(final long characteristic) {
        this.characteristic |= characteristic;
    }

    /** 判断字符串str是否和域名匹配（以域名结尾等） */
    public final boolean matchs(final String str) {
        return getMatcher().matchs(str, realDomain);
    }

    public final String getRealDomain() {
        return realDomain;
    }

    public final long getCharacteristic() {
        return characteristic;
    }

    /**
     * @return the trait
     */
    public String getName() {
        return name;
    }

    /**
     * @return the matcher
     */
    public final Matcher getMatcher() {
        return Matcher.get(this.matchType);
    }

    public int hashCode() {
        return Objects.hash(name, realDomain);
    }

    public boolean equals(final Object other) {
        if (other != null && other.getClass() == this.getClass()) {
            DomainPattern p = (DomainPattern) other;
            return this.name.equals(p.name) && this.realDomain.equals(p.realDomain)
                    && Objects.equals(this.category, p.category);
        }
        return false;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder()
                .append("{name:")
                .append(name)
                .append(",domain:")
                .append(realDomain)
                .append(",characteristic:")
                .append(Characteristics.toString(characteristic))
                .append(",Matcher:")
                .append(getMatcher())
                .append(",Category:")
                .append(category)
                .append("}");
        return sb.toString();
    }

    public final boolean isEqualMatchType() {
        return equalMatchType;
    }

    /**
     * @return the matchType
     */
    protected final String getMatchType() {
        return matchType;
    }

    /**
     * @param matchType the matchType to set
     */
    public void setMatchType(String matchType) {
        this.matchType = matchType;
        equalMatchType = Matcher.MATCH_EQUALS.equals(matchType);
    }

    /**
     * @return the serialversionuid
     */
    protected static long getSerialversionuid() {
        return serialVersionUID;
    }

    /**
     * @return the sp
     */
    protected static String getSp() {
        return SP;
    }

    /**
     * @param domain the domain to set
     */
    public final void setRealDomain(String domain) {
        this.realDomain = domain;
    }

    /**
     * @param characteristic the characteristic to set
     */
    protected final void setCharacteristic(long characteristic) {
        this.characteristic = characteristic;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }
}
