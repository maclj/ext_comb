package com.hadoop.entry;

/**
 * 任务输出key的定义（区分不同的项目）
 * 
 *
 */
public interface JobOutputSplitKey {

    /**
     * 返回区分标识。
     * @return
     */
    int getOutputSplitKey();
}
