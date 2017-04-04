package com.hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hadoop.entry.ClassFinder;
import com.hadoop.entry.Jobs;
import com.hadoop.plat.util.StringUtil;
import com.hadoop.util.Logger;

/**
 * 
 * 作为mapper的容器负责调用所管理的mapper。
 * 
 * 目前只支持KeyMapper类型（其实也可以支持普通的Mapper）。
 * 
 * 可以通过参数-Djob.comb.container="container1=map1,map2;container2=mapA,mapB"
 * 指定所有container包含的子类。
 * 
 * 
 * 
 *
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public abstract class ContainerKeyMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        implements ContainerMappers<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    /**
     * mapper 容器
     */
    // protected List<KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> mappers = new ArrayList<KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>>();
    
    /** key与 mapper的映射表 */
    protected Map<Integer, KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> keyMapperMap = new HashMap<Integer, KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>>();
    
    /** 老版本（未切换新平台）混合mapper */
    protected List<KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> elderMappers = new ArrayList<KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>>();

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
    public List<KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> createMappers(org.apache.hadoop.mapreduce.Mapper.Context context) {

        List<KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> values = new ArrayList<KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>>();
        
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
				values.add((KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>) obj);
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
    public void setup(Context context) throws IOException, InterruptedException {

        this.separator = Jobs.getOutputSeparator(context.getConfiguration());
        this.splitKey = getOutputSplitKey();// buildSplitKey(context.getConfiguration());
        this.context = getKeyMapContext(context, this.splitKey, this.separator);
        Logger.init(context.getConfiguration());

        List<KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> mappers = createMappers(context);
        for (KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper : mappers) {
            // setup
            mapper.setup(context); // 使用原来的context
            // cache
            if(mapper.isFitToOld()) {
                this.elderMappers.add(mapper);
            } else {
                this.keyMapperMap.put(mapper.getOutputSplitKey(), mapper);
            }
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
    protected void map(KEYIN key, VALUEIN text, Context context) throws IOException, InterruptedException {
        // 输入数据
        // String value = text.toString().trim();
        
        // map 需要判定value中是否包含splitKey
        boolean isSubSplitKey = text instanceof com.hadoop.io.SplitKey;
        // 利用toString返回实际值，自定义类型也需要利用toString返回实际值。
        String value = isSubSplitKey ? ((com.hadoop.io.SplitKey)text).toReal() : text.toString();

        // 输入数据是否存在key
        boolean isKeyDataIn = isKeyDataIn();

        Integer keyData = null;
        String valueData = null;
        // 输入数据没有携带key标识，通过container的isKeyDataIn获取
        if (!isKeyDataIn || this.separator == null) {
            // Nothing
            valueData = value; // 注意，这里不是自动判断的，而是需要子类声明输入数据有没有key标识，可以改进。
        } else {
            
            if (isSubSplitKey) {
                keyData = ((com.hadoop.io.SplitKey) text).getSplitKey();
                valueData = value; // 之前已经取过。
            } else {
                // 如果数据中有区分标识，则拆分并判断是否为需要处理的数据。
                List<String> splits = StringUtil.fastSplitToLimit(value, this.separator, 2);
                if (splits.size() == 0) {
                    return;
                }
                if (splits.size() != 2) {
                    return;
                }
                keyData = toInt(splits.get(0));
                valueData = splits.get(1);
            }
        }
        
        if(keyData != null) {
            KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper = this.keyMapperMap.get(keyData);
            if(mapper != null) {
                // 调整为只有SPLITKEY匹配的数据才处理
                mapper.map0(valueData, mapper.getWrappedContext());// 注意，要使用自己的context
            } else {
                // 找不到SPLITKEY对应的mapper实现
                writeDirectly(key, text, context);//使用原来的context
            }
            
        } else {
            // 没有SPLITKEY的情况，只能挨个调用
            for (KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper : this.keyMapperMap.values()) {
                mapper.map0(valueData, mapper.getWrappedContext());// 注意，要使用自己的context
            }
        }
        if (!this.elderMappers.isEmpty()) {
            // 继续兼容未实现平台的mapper
            for (KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> oldMapper : this.elderMappers) {
                oldMapper.map(key, text, context);// 使用原来的context
            }
        }

    }

    @Override
    public void map0(String value, Context context) throws IOException, InterruptedException {
        // Nothing
    }

    /**
     * Called once at the start of the task.
     */
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        
        for (KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper : this.keyMapperMap.values()) {
            mapper.cleanup(mapper.getWrappedContext());// 注意，要使用自己的context
        }

        if (!this.elderMappers.isEmpty()) {
            // 继续兼容未实现平台的reducer
            for (KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper : this.elderMappers) {
                mapper.cleanup(context);// 使用原来的context
            }
        }
        
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
     * container 缺省实现为true，认为输入数据存在key，需要拆分。
     * 
     * @return
     */
    protected boolean isKeyDataIn() {
        return true;
    }

    /**
     * container 自己不应该有输出业务，缺省实现为false，根据需要覆写。
     * 
     * @return
     */
    protected boolean isKeyDataOut() {
        return false;
    }

    /**
     * 返回整型。
     * @param input
     * @return
     */
    private Integer toInt(String input) {
        try {
            return Integer.valueOf(input);
        } catch(Exception e) {
            return null;
        }
    }
    
    /**
     * 将输入和输出值直接输出，需要满足以下条件：
     * 1、明确知道对应类型的转换方式；
     * 2、或者类型完全一致。
     * 
     * 实现该方法时需要判定条件。
     * @param key
     * @param values
     * @param context
     */
    protected void writeDirectly(KEYIN key, VALUEIN value, Context context)
            throws IOException, InterruptedException {
        // nothing
    }
}
