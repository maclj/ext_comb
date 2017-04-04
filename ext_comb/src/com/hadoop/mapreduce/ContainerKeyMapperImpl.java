package com.hadoop.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.hadoop.entry.comb.CombContainer;
import com.hadoop.io.SplitOutputKey;

/**
 * ContainerKeyMapper 缺省实现。 <br>
 * 支持 LongWritable, Text, SplitOutputKey, Text 的输入输出组合。<br>
 * 
 * 注意：
 * 输入数据若找不到对应实现的Mapper，则缺省丢弃。
 * 
 * 
 *
 */
@CombContainer(
        name = "containerKeyMapperImpl"
)
public class ContainerKeyMapperImpl extends ContainerKeyMapper<LongWritable, Text, SplitOutputKey, Text> {

}
