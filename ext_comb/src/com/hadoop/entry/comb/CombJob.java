package com.hadoop.entry.comb;

/**
 * 合并任务，仅定义用于搜索的名称和阶段定义。<br>
 * 
 * 其他job的详细配置，需要参考JobDefine中的配置。<br>
 * 
 * 与JobDefine必须同时存在。
 * 
 * 
 *
 */
@Deprecated
public @interface CombJob {

    /**
     * 用于搜索的标识信息。
     * @return
     */
    String name();
}
