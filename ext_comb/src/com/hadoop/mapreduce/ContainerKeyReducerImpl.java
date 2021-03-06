package com.hadoop.mapreduce;

import org.apache.hadoop.io.Text;

import com.hadoop.entry.comb.CombContainer;
import com.hadoop.io.SplitOutputKey;

/**
 * ContainerKeyReducer 缺省实现。 <br>
 * 支持 SplitOutputKey, Text, Text, Text 的输入输出组合。<br>
 * 注意：
 * 输入数据若找不到对应实现的Reducer，则缺省丢弃。
 * 
 * 
 *
 */
@CombContainer(
        name = "containerKeyReducerImpl"
)
public class ContainerKeyReducerImpl extends ContainerKeyReducer<SplitOutputKey, Text, Text, Text> {

}
