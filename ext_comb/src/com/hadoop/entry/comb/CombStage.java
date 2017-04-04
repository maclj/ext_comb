package com.hadoop.entry.comb;

/**
 * 合并任务的各阶段实现定义，通过-D参数获取并加载。<br>
 * 
 * 
 *
 */
@Deprecated
public @interface CombStage {

    /**
     * 用于搜索的标识信息。
     * @return
     */
    String name();
}
