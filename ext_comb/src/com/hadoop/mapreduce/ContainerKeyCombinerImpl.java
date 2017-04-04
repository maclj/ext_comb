package com.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.hadoop.entry.Jobs;
import com.hadoop.entry.comb.CombContainer;
import com.hadoop.io.SplitOutputKey;

/**
 * ContainerKeyCombiner 缺省实现。 <br>
 * 支持 SplitOutputKey, Text, SplitOutputKey, Text 的输入输出组合。<br>
 * 注意：
 * 输入数据若找不到对应实现的Combiner，则缺省直接输出
 * 
 * 
 *
 */
@CombContainer(
        name = "containerKeyCombinerImpl"
)
public class ContainerKeyCombinerImpl extends ContainerKeyReducer<SplitOutputKey, Text, SplitOutputKey, Text> {

    /**
     * {@inheritDoc}
     */
    protected void writeDirectly(SplitOutputKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text in : values) {
            context.write(key, in);
        }
    }
    
    /**
     * 创建解析器
     * @param context
     * @return
     */
    protected ContainerResolver createResolver(Context context) {
        boolean showWarn = Jobs.isWarnWhenContainIncomplete(context.getConfiguration());
        ContainerResolver resolver = new ContainerResolver()
                .withConf(context.getConfiguration())
                .withClass(getClass())
                .withWarn(showWarn)
                .withCombiner(); // 必须设置类型
        return resolver;
    }
}
