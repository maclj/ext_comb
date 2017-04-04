package com.hadoop.plat.mapreduce;

import org.apache.hadoop.io.Text;

import com.hadoop.entry.comb.CombContainer;
import com.hadoop.io.SplitOutputKey;

@CombContainer(
        name = "containerLogMapperImpl"
)
public class ContainerLogMapperImpl extends ContainerLogMapper<SplitOutputKey, Text> {

}
