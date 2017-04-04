package com.hadoop.plat.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.hadoop.entry.Jobs;
import com.hadoop.mapreduce.KeyOutMapper;
import com.hadoop.plat.log.factory.Factory;
import com.hadoop.plat.log.factory.Log;
import com.hadoop.plat.log.factory.LogFactory;

/**
 * 整合日志解析，实现为template方法，业务实现自己的业务方法。
 * 
 * 继承至KeyOutMapper，要求输出内容携带区分标识。
 * 
 * 
 *
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public abstract class LogMapper<KEYOUT, VALUEOUT> extends KeyOutMapper<LongWritable, Text,KEYOUT, VALUEOUT> {

    /** 日志解析工厂 */
    protected LogFactory factory = null;

    /**
     * Called once at the beginning of the task.
     */
    @Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {
        // 先把工厂创建了，防止子类遗漏调用super方法。
        this.factory = Factory.create(context.getConfiguration());
        if (this.factory == null) {
            throw new IllegalArgumentException(
                    "cannot init user log factory by " + context.getConfiguration().getInt(Jobs.JOB_USERLOG_TYPE, 0));
        }
        super.setup(context);
    }

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     */
    public void map0(String value, Context context) throws IOException, InterruptedException {
        // 解析用户日志
        Log log = this.factory.parse(value);
        // 注意，log为null，也继续调用子类，有些业务需要对日志内容进行校验。
//        if (log == null) {
//            return;
//        }
        // 调用业务
        map0(log, value, context);

    }

    /**
     * 业务需要实现的方法。
     * 
     * @param log
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    protected abstract void map0(Log log, String value, Context context) throws IOException, InterruptedException;
    
    /**
     * Called once at the end of the task.
     */
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);
    }

}
