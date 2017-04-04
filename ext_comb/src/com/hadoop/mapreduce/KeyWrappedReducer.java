package com.hadoop.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

import com.hadoop.entry.JobOutputSplitKey;
import com.hadoop.entry.Jobs;
import com.hadoop.io.DynamicSplitKey;
import com.hadoop.plat.util.StringUtil;
import com.hadoop.util.Logger;

/**
 * 适配输入输出数据种可能携带区分标识key的情况。
 * 
 * 目前KEY 类型支持：
 * 1、Text
 * 2、com.hadoop.io.SplitKey 子类实现
 * 3、com.hadoop.io.DynamicSplitKey 子类实现
 * 
 * 
 *
 * @param <KEYIN>
 * @param <VALUEIN>
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public abstract class KeyWrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        implements JobOutputSplitKey {
    
    /** context */
    protected Context context;

    /** 数据的版本或者标识 */
    protected int splitKey;

    /** 输出分隔符 */
    protected String separator;

    /**
     * 子类不允许实现该方法，调整为实现setup0方法。
     */
    protected void setup(Context context) throws IOException, InterruptedException {

        this.splitKey = getOutputSplitKey();//buildSplitKey(context.getConfiguration());
        this.separator = Jobs.getOutputSeparator(context.getConfiguration());
        initContext(context);
        Logger.init(context.getConfiguration());    
        setup0(this.context);
    }
    
    /**
     * 初始化context，定制mrunit使用
     */
    protected void initContext(Context context) {
        this.context = getReducerContext(context, getOutputSplitKey(), this.separator);
    }
    
    /**
     * 子类不允许实现该方法，调整为实现reduce0方法。
     * 
     */
    protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context)
            throws IOException, InterruptedException {
        
        // reduce 需要判定key中是否包含splitKey
        boolean isSubSplitKey = key instanceof com.hadoop.io.SplitKey;
        String realKey = null;
        if(isSubSplitKey) {
            realKey = ((com.hadoop.io.SplitKey)key).toReal();
        } else if(key instanceof DynamicSplitKey) {
            realKey = ((DynamicSplitKey)key).toReal(); // 兼容
        } else {
            realKey = key.toString();
        }
        if(realKey == null) {
            // ignored
            return;
        }
        
        boolean isKeyDataIn = isKeyDataIn();
        if (isKeyDataIn && this.separator != null && this.splitKey != 0) {
            
            if (isSubSplitKey) {
                int keyData = ((com.hadoop.io.SplitKey) key).getSplitKey();
                boolean bool = isKeyOf(keyData);
                if (!bool) {
                    return;
                }
            } else {

                List<String> splits = StringUtil.fastSplitToLimit(realKey, this.separator, 2);
                if (splits.size() == 0) {
                    return;
                }
                if (splits.size() != 2) {
                    return;
                }
                String keyData = splits.get(0);
                boolean bool = isKeyOf(keyData);
                if (!bool) {
                    return;
                }
                realKey = splits.get(1);
            }
        }
        // 实际子类需要实现的方法。
        reduce0(realKey, key, values, this.context);
    }
    
    /**
     * 子类不允许实现该方法，调整为实现cleanup0方法。
     */
    protected void cleanup(Context context
                           ) throws IOException, InterruptedException {
        cleanup0(this.context);
    }
    
    /**
     * Called once at the start of the task.
     */
    public void setup0(Context context) throws IOException, InterruptedException {
        // NOTHING
    }

    /**
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation
     * is an identity function.
     * 
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract void reduce0(String key, Iterable<VALUEIN> values, Context context)
            throws IOException, InterruptedException;
    
    /**
     * 兼容需要原始输入key值的情况，如果需要应自行覆写。
     * 
     * 缺省实现为调用reduce0(String key, Iterable<VALUEIN> values, Context context)
     * 
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce0(String key, KEYIN oriKey, Iterable<VALUEIN> values, Context context)
            throws IOException, InterruptedException {
        reduce0(key, values, context);
    }
    
    
    /**
     * Called once at the end of the task.
     * 
     * 缺省不实现，只是为了统一所有的接口。
     * 子类若实现，必须实现该方法。
     */
    public void cleanup0(Context context
                           ) throws IOException, InterruptedException {
      // NOTHING
    }

    /**
     * 判断是否应该是自己处理的数据。
     * 
     * @param keyCfg
     * @param keyData
     * @return
     */
    public boolean isKeyOf(String keyData) {
        if (this.splitKey == 0) {
            // 没有配置，则认为都符合
            return true;
        }
        
        try {
            int tmp = Integer.parseInt(keyData);
            return this.splitKey == tmp;
            
        } catch(Exception e) {
            return false;
        }

    }
    
    /**
     * 判断是否应该是自己处理的数据。
     * 
     * @param keyCfg
     * @param keyData
     * @return
     */
    public boolean isKeyOf(int keyData) {
        if (this.splitKey == 0) {
            // 没有配置，则认为都符合
            return true;
        }
        return this.splitKey == keyData;
    }

    /**
     * 标识输入数据中是否含有区分标识（第一个分隔符之前的内容是否为区分标识）
     * 
     * @return
     */
    protected abstract boolean isKeyDataIn();

    /**
     * 输出数据时是否需要包含区分标识。
     * 
     * @return
     */
    protected abstract boolean isKeyDataOut();
    
    /**
     * 在容器内运行时，为了区分是否为按照KeyWrappedReducer流程开发的新版本，还是仅仅套个壳，仍然为旧有模式的实现。
     * 
     * isFitToOld 为true时，说明实现者的setup、reduce、cleanup方法均直接覆写了Mapper的对应方法，
     * 如果要走KeyWrappedReducer流程，isFitToOld 必须为false.
     * 
     * @return
     */
    protected boolean isFitToOld() {
        return false;
    }
    
    /**
     * 获取该Job对应的区分key标识，缺省通过Configuration。
     * 子类可以扩展，但是应该保持优先级，即JobDefine定义的要优先于子类中定义的。
     * 
     * @param conf
     * @return
     */
//    protected String buildSplitKey(Configuration conf) {
//        // 优先取自己类的定义
//        String key = Jobs.getSplitKeyByClass(conf, getClass());
//        if(key == null) {
//            // 取缺省类的定义
//            key = Jobs.getSplitKeyByClass(conf, Reducer.class);
//        }
//        if(key == null) {
//            // 取子类的定义
//            key = getUserDefinedSplitKey(conf);
//        }
//        return key;
//    }
    
    /**
     * 获取当前区分的key标识。
     * @return
     */
//    public String getSplitKey() {
//        return this.splitKey;
//    }
    
    /**
     * 子类定义的特殊拆分key，优先返回配置中的key
     * @param conf
     * @return
     */
    protected String getUserDefinedSplitKey(Configuration conf) {
        return null;
    }
    
    /**
     * 获取当前容器内的Context
     * @return
     */
    public Context getWrappedContext() {
        return this.context;
    }

    /**
     * A a wrapped {@link Reducer.Context} for custom implementations.
     * 
     * @param reduceContext
     *            <code>ReduceContext</code> to be wrapped
     * @return a wrapped <code>Reducer.Context</code> for custom implementations
     */
    public Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context getReducerContext(
            ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext, int key, String separator) {
        return new KeyReducerContext(reduceContext, key, separator);
    }

    /**
     * wrapped context
     * 
     * 
     *
     */
    public class KeyReducerContext extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {

        /** context */
        protected ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext;
        
        /** 数据的版本或者标识 */
        protected int key;
        
        /** 输出分隔符 */
        protected String separator;

        public KeyReducerContext(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext, int key, String separator) {
            this.reduceContext = reduceContext;
            this.key = key;
            this.separator = separator;
        }
        
        /**
         * 追加区分的key标识到输出内容中，目前只适配Text和OutputKey
         * 
         * @param keyout
         * @return
         */
        @SuppressWarnings("unchecked")
        public KEYOUT addSplitKey(KEYOUT keyout) {
            // 没有key标识，则不添加
            if (this.key == 0) {
                return keyout;
            }
            
            // 是SplitKey
            if (keyout instanceof com.hadoop.io.SplitKey) {
                com.hadoop.io.SplitKey tmp = ((com.hadoop.io.SplitKey) keyout);
                tmp.setSplitKey(this.key);
                return (KEYOUT) tmp;
            }

            // 是Text
            if (Text.class.isAssignableFrom(keyout.getClass())) {
                Text tmp = (Text) keyout;
                return (KEYOUT) new Text(this.key + this.separator + tmp.toString());
            }
            
            if (keyout instanceof DynamicSplitKey) {
                DynamicSplitKey tmp = ((DynamicSplitKey) keyout).append(this.key, this.separator);
                return (KEYOUT) tmp;
            }
            // 其余类型不增加
            return keyout;
        }
        
        @Override
        public void write(KEYOUT key, VALUEOUT value) throws IOException, InterruptedException {
            // 需要补充区分标识时才追加key
            KEYOUT wrapped = isKeyDataOut() ? addSplitKey(key) : key;
            if (wrapped == null) {
                return;
            }
            reduceContext.write(wrapped, value);
        }
        
        @Override
        public KEYIN getCurrentKey() throws IOException, InterruptedException {
            return reduceContext.getCurrentKey();
        }

        @Override
        public VALUEIN getCurrentValue() throws IOException, InterruptedException {
            return reduceContext.getCurrentValue();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return reduceContext.nextKeyValue();
        }

        @SuppressWarnings("rawtypes")
        @Override
        public Counter getCounter(Enum counterName) {
            return reduceContext.getCounter(counterName);
        }

        @Override
        public Counter getCounter(String groupName, String counterName) {
            return reduceContext.getCounter(groupName, counterName);
        }

        @Override
        public OutputCommitter getOutputCommitter() {
            return reduceContext.getOutputCommitter();
        }

        @Override
        public String getStatus() {
            return reduceContext.getStatus();
        }

        @Override
        public TaskAttemptID getTaskAttemptID() {
            return reduceContext.getTaskAttemptID();
        }

        @Override
        public void setStatus(String msg) {
            reduceContext.setStatus(msg);
        }

        @Override
        public Path[] getArchiveClassPaths() {
            return reduceContext.getArchiveClassPaths();
        }

        @Override
        public String[] getArchiveTimestamps() {
            return reduceContext.getArchiveTimestamps();
        }

        @Override
        public URI[] getCacheArchives() throws IOException {
            return reduceContext.getCacheArchives();
        }

        @Override
        public URI[] getCacheFiles() throws IOException {
            return reduceContext.getCacheFiles();
        }

        @Override
        public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
            return reduceContext.getCombinerClass();
        }

        @Override
        public Configuration getConfiguration() {
            return reduceContext.getConfiguration();
        }

        @Override
        public Path[] getFileClassPaths() {
            return reduceContext.getFileClassPaths();
        }

        @Override
        public String[] getFileTimestamps() {
            return reduceContext.getFileTimestamps();
        }

        @Override
        public RawComparator<?> getCombinerKeyGroupingComparator() {
            return reduceContext.getCombinerKeyGroupingComparator();
        }

        @Override
        public RawComparator<?> getGroupingComparator() {
            return reduceContext.getGroupingComparator();
        }

        @Override
        public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
            return reduceContext.getInputFormatClass();
        }

        @Override
        public String getJar() {
            return reduceContext.getJar();
        }

        @Override
        public JobID getJobID() {
            return reduceContext.getJobID();
        }

        @Override
        public String getJobName() {
            return reduceContext.getJobName();
        }

        @Override
        public boolean getJobSetupCleanupNeeded() {
            return reduceContext.getJobSetupCleanupNeeded();
        }

        @Override
        public boolean getTaskCleanupNeeded() {
            return reduceContext.getTaskCleanupNeeded();
        }

        @SuppressWarnings("deprecation")
        @Override
        public Path[] getLocalCacheArchives() throws IOException {
            return reduceContext.getLocalCacheArchives();
        }

        @SuppressWarnings("deprecation")
        @Override
        public Path[] getLocalCacheFiles() throws IOException {
            return reduceContext.getLocalCacheFiles();
        }

        @Override
        public Class<?> getMapOutputKeyClass() {
            return reduceContext.getMapOutputKeyClass();
        }

        @Override
        public Class<?> getMapOutputValueClass() {
            return reduceContext.getMapOutputValueClass();
        }

        @Override
        public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
            return reduceContext.getMapperClass();
        }

        @Override
        public int getMaxMapAttempts() {
            return reduceContext.getMaxMapAttempts();
        }

        @Override
        public int getMaxReduceAttempts() {
            return reduceContext.getMaxReduceAttempts();
        }

        @Override
        public int getNumReduceTasks() {
            return reduceContext.getNumReduceTasks();
        }

        @Override
        public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
            return reduceContext.getOutputFormatClass();
        }

        @Override
        public Class<?> getOutputKeyClass() {
            return reduceContext.getOutputKeyClass();
        }

        @Override
        public Class<?> getOutputValueClass() {
            return reduceContext.getOutputValueClass();
        }

        @Override
        public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
            return reduceContext.getPartitionerClass();
        }

        @Override
        public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
            return reduceContext.getReducerClass();
        }

        @Override
        public RawComparator<?> getSortComparator() {
            return reduceContext.getSortComparator();
        }

        @SuppressWarnings("deprecation")
        @Override
        public boolean getSymlink() {
            return reduceContext.getSymlink();
        }

        @Override
        public Path getWorkingDirectory() throws IOException {
            return reduceContext.getWorkingDirectory();
        }

        @Override
        public void progress() {
            reduceContext.progress();
        }

        @Override
        public Iterable<VALUEIN> getValues() throws IOException, InterruptedException {
            return reduceContext.getValues();
        }

        @Override
        public boolean nextKey() throws IOException, InterruptedException {
            return reduceContext.nextKey();
        }

        @Override
        public boolean getProfileEnabled() {
            return reduceContext.getProfileEnabled();
        }

        @Override
        public String getProfileParams() {
            return reduceContext.getProfileParams();
        }

        @Override
        public IntegerRanges getProfileTaskRange(boolean isMap) {
            return reduceContext.getProfileTaskRange(isMap);
        }

        @Override
        public String getUser() {
            return reduceContext.getUser();
        }

        @Override
        public Credentials getCredentials() {
            return reduceContext.getCredentials();
        }

        @Override
        public float getProgress() {
            return reduceContext.getProgress();
        }
    }
}
