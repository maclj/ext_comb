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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

import com.hadoop.entry.JobOutputSplitKey;
import com.hadoop.entry.Jobs;
import com.hadoop.io.DynamicSplitKey;
import com.hadoop.plat.util.StringUtil;
import com.hadoop.util.Logger;

/**
 * 输入为LongWritable 和 Text。
 * 
 * 目前VALUEIN类型支持：
 * 1、Text
 * 不支持其它类型，需要配合LineRecorReader、LindReader等自定义序列化类型读取实现。
 * 
 * 
 *
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public abstract class KeyWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        implements JobOutputSplitKey {

    /** 数据的版本或者标识 */
    protected int splitKey;

    /** 输出分隔符 */
    protected String separator;

    /** 上下文 */
    protected org.apache.hadoop.mapreduce.Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context;

    /**
     * 子类不允许实现该方法，调整为实现setup0方法。
     */
    protected void setup(org.apache.hadoop.mapreduce.Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {
        this.separator = Jobs.getOutputSeparator(context.getConfiguration());
        this.splitKey = getOutputSplitKey();//buildSplitKey(context.getConfiguration());
        initContext(context);
        Logger.init(context.getConfiguration());
        setup0(this.context);
    }

    /**
     * 初始化context，定制mrunit使用
     */
    protected void initContext(Context context) {
        this.context = getKeyMapContext(context, getOutputSplitKey(), this.separator);
    }
    /**
     * 子类不允许实现该方法，调整为实现map0方法。
     * 
     * @param key
     * @param text
     * @param context
     */
    protected void map(KEYIN key, VALUEIN text, Context context)
            throws IOException, InterruptedException {
        // map 需要判定value中是否包含splitKey
        boolean isSubSplitKey = text instanceof com.hadoop.io.SplitKey;
        // 利用toString返回实际值，自定义类型也需要利用toString返回实际值。
        String value = isSubSplitKey ? ((com.hadoop.io.SplitKey)text).toReal() : text.toString();
        
        // 
        boolean isKeyDataIn = isKeyDataIn();
        if (isKeyDataIn && this.separator != null && this.splitKey != 0) {// 无拆分标识，则约定为都处理
            // 如果数据中有区分标识，则拆分并判断是否为需要处理的数据。
            if (isSubSplitKey) {
                int keyData = ((com.hadoop.io.SplitKey) text).getSplitKey();
                boolean bool = isKeyOf(keyData);
                if (!bool) {
                    return;
                }
            } else {
                List<String> splits = StringUtil.fastSplitToLimit(value, this.separator, 2);
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
                value = splits.get(1);
            }
        }
        
        // 实际子类需要实现的方法。
        map0(value, this.context);
    }
    
    /**
     * Called once at the end of the task.
     */
    protected void cleanup(org.apache.hadoop.mapreduce.Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {
        cleanup0(this.context);
    }
    
    /**
     * Called once at the start of the task.
     */
    public void setup0(org.apache.hadoop.mapreduce.Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context) throws IOException, InterruptedException {
        // NOTHING
    }

    /**
     * 处理实际的数据，要求子类必须实现该方法。
     * Called once for each key/value pair in the input split. 
     * Most applications should override this, but the default is the identity function.
     * 
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract void map0(String value,
            org.apache.hadoop.mapreduce.Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException;

    /**
     * 缺省不实现，只是为了统一所有的接口。
     * 子类若实现，必须实现该方法。
     * 
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void cleanup0(org.apache.hadoop.mapreduce.Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {

    }

    /**
     * 判断是否应该是自己处理的数据。
     * 
     * 注意缺省实现为，如果没有提供定义的区分key内容，则无论数据中key标识为什么，都认为是需要处理的。
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
     * @return
     */
    protected abstract boolean isKeyDataIn();
    
    /**
     * 输出数据时是否需要包含区分标识。
     * @return
     */
    protected abstract boolean isKeyDataOut();
    
    /**
     * 在容器内运行时，为了区分是否为按照KeyWrappedMapper流程开发的新版本，还是仅仅套个壳，仍然为旧有模式的实现。
     * 
     * isFitToOld 为true时，说明实现者的setup、map、cleanup方法均直接覆写了Mapper的对应方法，
     * 如果要走KeyWrappedMapper流程，isFitToOld 必须为false.
     * 
     * @return
     */
    protected boolean isFitToOld() {
        return false;
    }
    
//    /**
//     * 获取该Job对应的区分key标识，缺省通过Configuration。
//     * 子类可以扩展，但是应该保持优先级，即JobDefine定义的要优先于子类中定义的。
//     * 
//     * @param conf
//     * @return
//     */
//    protected String buildSplitKey(Configuration conf) {
//        // 优先取自己类的定义
//        String key = Jobs.getSplitKeyByClass(conf, getClass());
//        if(key == null) {
//            // 取缺省类的定义
//            key = Jobs.getSplitKeyByClass(conf, Mapper.class);
//        }
//        if(key == null) {
//            // 取子类的定义
//            key = getUserDefinedSplitKey(conf);
//        }
//        return key;
//    }
    
    
//    /**
//     * 子类定义的特殊拆分key，优先返回配置中的key
//     * @param conf
//     * @return
//     */
//    protected String getUserDefinedSplitKey(Configuration conf) {
//        return null;
//    }
    
    /**
     * 获取当前容器内的Context
     * @return
     */
    public Context getWrappedContext() {
        return this.context;
    }

    /**
     * Get a wrapped {@link Mapper.Context} for custom implementations.
     * 
     * @param mapContext
     *            <code>MapContext</code> to be wrapped
     * @return a wrapped <code>Mapper.Context</code> for custom implementations
     */
    public Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context getKeyMapContext(
            Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context mapContext, int key, String separator) {
        return new KeyMapContext(mapContext, key, separator);
    }

    /**
     * wrapped context
     * 
     * 
     *
     */
    public class KeyMapContext extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {

        /** wrapped */
        protected MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext;

        /** 输出分隔符 */
        protected String separator;

        /** 数据的版本或者标识 */
        protected int key;

        /**
         * 构造函数
         * 
         * @param mapContext
         * @param key
         * @param separator
         */
        public KeyMapContext(Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context mapContext, int key,
                String separator) {
            this.mapContext = mapContext;
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
            
            // 是DynamicSplitKey
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
            mapContext.write(wrapped, value);
        }

        /**
         * Get the input split for this map.
         */
        public InputSplit getInputSplit() {
            return mapContext.getInputSplit();
        }

        @Override
        public KEYIN getCurrentKey() throws IOException, InterruptedException {
            return mapContext.getCurrentKey();
        }

        @Override
        public VALUEIN getCurrentValue() throws IOException, InterruptedException {
            return mapContext.getCurrentValue();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return mapContext.nextKeyValue();
        }

        @Override
        public Counter getCounter(Enum<?> counterName) {
            return mapContext.getCounter(counterName);
        }

        @Override
        public Counter getCounter(String groupName, String counterName) {
            return mapContext.getCounter(groupName, counterName);
        }

        @Override
        public OutputCommitter getOutputCommitter() {
            return mapContext.getOutputCommitter();
        }

        @Override
        public String getStatus() {
            return mapContext.getStatus();
        }

        @Override
        public TaskAttemptID getTaskAttemptID() {
            return mapContext.getTaskAttemptID();
        }

        @Override
        public void setStatus(String msg) {
            mapContext.setStatus(msg);
        }

        @Override
        public Path[] getArchiveClassPaths() {
            return mapContext.getArchiveClassPaths();
        }

        @Override
        public String[] getArchiveTimestamps() {
            return mapContext.getArchiveTimestamps();
        }

        @Override
        public URI[] getCacheArchives() throws IOException {
            return mapContext.getCacheArchives();
        }

        @Override
        public URI[] getCacheFiles() throws IOException {
            return mapContext.getCacheFiles();
        }

        @Override
        public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
            return mapContext.getCombinerClass();
        }

        @Override
        public Configuration getConfiguration() {
            return mapContext.getConfiguration();
        }

        @Override
        public Path[] getFileClassPaths() {
            return mapContext.getFileClassPaths();
        }

        @Override
        public String[] getFileTimestamps() {
            return mapContext.getFileTimestamps();
        }

        @Override
        public RawComparator<?> getCombinerKeyGroupingComparator() {
            return mapContext.getCombinerKeyGroupingComparator();
        }

        @Override
        public RawComparator<?> getGroupingComparator() {
            return mapContext.getGroupingComparator();
        }

        @Override
        public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
            return mapContext.getInputFormatClass();
        }

        @Override
        public String getJar() {
            return mapContext.getJar();
        }

        @Override
        public JobID getJobID() {
            return mapContext.getJobID();
        }

        @Override
        public String getJobName() {
            return mapContext.getJobName();
        }

        @Override
        public boolean getJobSetupCleanupNeeded() {
            return mapContext.getJobSetupCleanupNeeded();
        }

        @Override
        public boolean getTaskCleanupNeeded() {
            return mapContext.getTaskCleanupNeeded();
        }

        @SuppressWarnings("deprecation")
        @Override
        public Path[] getLocalCacheArchives() throws IOException {
            return mapContext.getLocalCacheArchives();
        }

        @SuppressWarnings("deprecation")
        @Override
        public Path[] getLocalCacheFiles() throws IOException {
            return mapContext.getLocalCacheFiles();
        }

        @Override
        public Class<?> getMapOutputKeyClass() {
            return mapContext.getMapOutputKeyClass();
        }

        @Override
        public Class<?> getMapOutputValueClass() {
            return mapContext.getMapOutputValueClass();
        }

        @Override
        public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
            return mapContext.getMapperClass();
        }

        @Override
        public int getMaxMapAttempts() {
            return mapContext.getMaxMapAttempts();
        }

        @Override
        public int getMaxReduceAttempts() {
            return mapContext.getMaxReduceAttempts();
        }

        @Override
        public int getNumReduceTasks() {
            return mapContext.getNumReduceTasks();
        }

        @Override
        public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
            return mapContext.getOutputFormatClass();
        }

        @Override
        public Class<?> getOutputKeyClass() {
            return mapContext.getOutputKeyClass();
        }

        @Override
        public Class<?> getOutputValueClass() {
            return mapContext.getOutputValueClass();
        }

        @Override
        public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
            return mapContext.getPartitionerClass();
        }

        @Override
        public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
            return mapContext.getReducerClass();
        }

        @Override
        public RawComparator<?> getSortComparator() {
            return mapContext.getSortComparator();
        }

        @SuppressWarnings("deprecation")
        @Override
        public boolean getSymlink() {
            return mapContext.getSymlink();
        }

        @Override
        public Path getWorkingDirectory() throws IOException {
            return mapContext.getWorkingDirectory();
        }

        @Override
        public void progress() {
            mapContext.progress();
        }

        @Override
        public boolean getProfileEnabled() {
            return mapContext.getProfileEnabled();
        }

        @Override
        public String getProfileParams() {
            return mapContext.getProfileParams();
        }

        @Override
        public IntegerRanges getProfileTaskRange(boolean isMap) {
            return mapContext.getProfileTaskRange(isMap);
        }

        @Override
        public String getUser() {
            return mapContext.getUser();
        }

        @Override
        public Credentials getCredentials() {
            return mapContext.getCredentials();
        }

        @Override
        public float getProgress() {
            return mapContext.getProgress();
        }
    }
}
