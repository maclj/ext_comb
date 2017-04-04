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
 * 作为reducer的容器负责调用所管理的reducer。
 * 
 * 目前只支持KeyWrappedReducer类型（其实也可以支持普通的Reducer）。
 * 
 * 
 *
 * @param <KEYIN>
 * @param <VALUEIN>
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public abstract class ContainerKeyReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        extends KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
        implements ContainerReducers<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{

    /**
     * mapper 容器
     */
    // protected List<KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> reducers = new ArrayList<KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>>();
    
    /** key与 reducer的映射表 */
    protected Map<Integer, KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> keyReducerMap = new HashMap<Integer, KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>>();
    
    /** 老版本（未切换新平台）混合reducer */
    protected List<KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> elderReducers = new ArrayList<KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>>();

    
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
                .withReducer(); // 必须设置类型
        return resolver;
    }
    
    /**
     * 缺省实现为根据annotation的来获取管理对象的实现。
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public List<KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> createReducers(org.apache.hadoop.mapreduce.Reducer.Context context) {
        
		List<KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> values = new ArrayList<KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>>();

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
			if (obj instanceof KeyWrappedReducer) {
				values.add((KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>) obj);
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
        
        this.splitKey = getOutputSplitKey();//buildSplitKey(context.getConfiguration());
        this.separator = Jobs.getOutputSeparator(context.getConfiguration());
        this.context = getReducerContext(context, this.splitKey, this.separator);
        Logger.init(context.getConfiguration());    
        // this.reducers.addAll(createReducers(context));
        List<KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> reducers = createReducers(context);
        for (KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer : reducers) {
            // setup
            reducer.setup(context); // 使用原来的context
            // cache
            if(reducer.isFitToOld()) {
                this.elderReducers.add(reducer);
            } else {
                this.keyReducerMap.put(reducer.getOutputSplitKey(), reducer);
            }
        }
        // 容器类不应该有自己的业务逻辑，建议不实现。
        setup0(this.context);
    }

    /**
     * 子类不允许实现该方法，调整为实现reduce0方法。
     * 
     */
    @Override
    protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context)
            throws IOException, InterruptedException {

        // reduce 需要判定key中是否包含splitKey
        boolean isSubSplitKey = key instanceof com.hadoop.io.SplitKey;
        String value = isSubSplitKey ? ((com.hadoop.io.SplitKey)key).toReal() :key.toString();
        boolean isKeyDataIn = isKeyDataIn();

        Integer keyData = null;
        String valueData = null;
        // 输入数据没有携带key标识，通过container的isKeyDataIn获取
        if (!isKeyDataIn || this.separator == null) {
            // Nothing
            valueData = value;
        } else {
            
            if (isSubSplitKey) {
                keyData = ((com.hadoop.io.SplitKey) key).getSplitKey();
                valueData = value;
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
            KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer = this.keyReducerMap.get(keyData);
            if(reducer != null) {
                // 调整为只有SPLITKEY匹配的数据才处理
                reducer.reduce0(valueData, key, values, reducer.getWrappedContext());// 注意，要使用reducer自己的context
            } else {
                // 找不到SPLITKEY对应的reducer实现
                writeDirectly(key, values, context);//使用原来的context
            }
            
        } else {
            // 没有SPLITKEY的情况，只能挨个调用
            for (KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer : this.keyReducerMap.values()) {
                reducer.reduce0(valueData, key, values, reducer.getWrappedContext());// 注意，要使用reducer自己的context
            }
        }
        if (!this.elderReducers.isEmpty()) {
            // 继续兼容未实现平台的reducer
            for (KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> tmpReducer : this.elderReducers) {
                tmpReducer.reduce(key, values, context);// 使用原来的context
            }
        }
    }
    
    @Override
    public void reduce0(String key, Iterable<VALUEIN> values, Context context)
            throws IOException, InterruptedException {
        //Nothing
    }

    /**
     * Called once at the end of the task.
     * 
     * 缺省不实现，只是为了统一所有的接口。 子类若实现，必须实现该方法。
     */
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        
        // 没有SPLITKEY的情况，只能挨个调用
        for (KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer : this.keyReducerMap.values()) {
            reducer.cleanup(reducer.getWrappedContext());// 注意，要使用reducer自己的context
        }

        if (!this.elderReducers.isEmpty()) {
            // 继续兼容未实现平台的reducer
            for (KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> tmpReducer : this.elderReducers) {
                tmpReducer.cleanup(context);// 使用原来的context
            }
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
     * container 缺省实现为true，认为输入数据存在key，需要拆分。
     */
    @Override
    protected boolean isKeyDataIn() {
        return true;
    }

    /**
     * container 自己不应该有输出业务，缺省实现为false，根据需要覆写。
     */
    @Override
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
    protected void writeDirectly(KEYIN key, Iterable<VALUEIN> values, Context context)
            throws IOException, InterruptedException {
        // nothing
    }
}
