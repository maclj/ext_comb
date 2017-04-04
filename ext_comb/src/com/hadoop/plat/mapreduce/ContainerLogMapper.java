package com.hadoop.plat.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.hadoop.entry.ClassFinder;
import com.hadoop.entry.Jobs;
import com.hadoop.mapreduce.ContainerMappers;
import com.hadoop.mapreduce.ContainerResolver;
import com.hadoop.mapreduce.KeyWrappedMapper;
import com.hadoop.plat.log.factory.Factory;
import com.hadoop.plat.log.factory.Log;
import com.hadoop.util.Logger;

/**
 * 容器，用于对同一条日志进行多次处理<br>
 * 
 * 注意：在容器内，原来的Mapper中的工厂以及构建流程都无用，仅用于支持两种形态都可以运行。
 * 
 * 
 *
 */
public class ContainerLogMapper<KEYOUT, VALUEOUT> extends LogMapper<KEYOUT, VALUEOUT>
        implements ContainerMappers<LongWritable, Text,KEYOUT, VALUEOUT> {
    /**
     * mapper 容器
     */
    protected List<LogMapper<KEYOUT, VALUEOUT>> mappers = new ArrayList<LogMapper<KEYOUT, VALUEOUT>>();

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
                .withMapper(); // 必须设置类型
        return resolver;
    }

    /**
     * 缺省实现为根据annotation的来获取管理对象的实现。
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public List<KeyWrappedMapper<LongWritable, Text, KEYOUT, VALUEOUT>> createMappers(org.apache.hadoop.mapreduce.Mapper.Context context) {
        
		List<KeyWrappedMapper<LongWritable, Text, KEYOUT, VALUEOUT>> values = new ArrayList<KeyWrappedMapper<LongWritable, Text, KEYOUT, VALUEOUT>>();

		boolean showWarn = Jobs.isWarnWhenContainIncomplete(context.getConfiguration());
		ContainerResolver resolver = createResolver(context);
		Class<?>[] clazzes = resolver.fetchClasses();
		if (showWarn && ContainerResolver.isUndefined(clazzes)) {
			throw new IllegalArgumentException("container is empty.");
		}

		Object obj = null;
		for (Class<?> clazz : clazzes) {
			obj = ClassFinder.newInstance(clazz);
			if (showWarn && obj == null) {
				throw new IllegalArgumentException(String.format("can't newInstance of %s.", clazz.getName()));
			}
			if (obj instanceof KeyWrappedMapper) {
				values.add((KeyWrappedMapper<LongWritable, Text, KEYOUT, VALUEOUT>) obj);
			}
		}
        if (showWarn && values.isEmpty()) {
            throw new IllegalArgumentException("container is empty.");
        }
        return values;
    }
    
    /**
     * Called once at the start of the task.
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 先把工厂创建了，防止子类遗漏调用super方法。
        this.factory = Factory.create(context.getConfiguration());
        if (this.factory == null) {
            throw new IllegalArgumentException(
                    "cannot init log factory");
        }

        this.separator = Jobs.getOutputSeparator(context.getConfiguration());
        this.splitKey = getOutputSplitKey();//buildSplitKey(context.getConfiguration());
        this.context = getKeyMapContext(context, getOutputSplitKey(), this.separator);
        Logger.init(context.getConfiguration());
        
        List<KeyWrappedMapper<LongWritable, Text, KEYOUT, VALUEOUT>> tmp = createMappers(context);
        if(tmp == null || tmp.size() == 0) {
            throw new IllegalArgumentException("failed to init mappers(KeyWrappedMapper).");
        }
        for(KeyWrappedMapper<LongWritable, Text, KEYOUT, VALUEOUT> kwm : tmp) {
            if(kwm instanceof LogMapper) {
                LogMapper<KEYOUT, VALUEOUT> mapper = (LogMapper<KEYOUT, VALUEOUT>)kwm;
                this.mappers.add(mapper);
                mapper.setup(context);
            }
        }
        if(mappers.size() == 0) {
            throw new IllegalArgumentException("failed to init mappers(UserlogMapper).");
        }
        // 容器类不应该有自己的业务逻辑，建议不实现。
        setup0(this.context);
    }

    /**
     * 子类不允许实现该方法，调整为实现map0方法。
     * 
     * @param key
     * @param text
     * @param context
     */
    @Override
    protected void map0(Log log, String value, Context context) throws IOException, InterruptedException {

        for (LogMapper<KEYOUT, VALUEOUT> mapper : mappers) {
            mapper.map0(log, value, mapper.getWrappedContext());
        }
    }

    /**
     * Called once at the start of the task.
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (LogMapper<KEYOUT, VALUEOUT> mapper : mappers) {
            mapper.cleanup(mapper.getWrappedContext());
        }
        // 容器类不应该有自己的业务逻辑，建议不实现。
        cleanup0(this.context);
    }
    
    /**
     * container 自己需要需要拆分key
     */
    @Override
    public int getOutputSplitKey() {
        return 0;
    }
    
    /**
     * userlog解析输入都是日志，不需要区分key，直接实现为false。
     * @return
     */
    protected boolean isKeyDataIn() {
        return false;
    }
    
    /**
     * container 自己不应该有输出业务，缺省实现为false，根据需要覆写。
     * @return
     */
    protected boolean isKeyDataOut() {
        return false;
    }

}
