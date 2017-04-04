package com.hadoop.entry.comb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoop.mapreduce.KeyWrappedMapper;
import com.hadoop.plat.util.StringUtil;

@Deprecated
public class KeyDynamicMapper<KEYOUT, VALUEOUT> extends KeyWrappedMapper<LongWritable, Text,KEYOUT, VALUEOUT> {
    
    @SuppressWarnings("rawtypes")
    protected List<Mapper> mappers = new ArrayList<Mapper>();

    /**
     * 子类不允许实现该方法，调整为实现setup0方法。
     */
    protected void setup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        
        
    }

    /**
     * 子类不允许实现该方法，调整为实现map0方法。
     * 
     * @param key
     * @param text
     * @param context
     */
    protected void map(LongWritable key, Text text,
            org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {

        String value = text.toString().trim();
        boolean isKeyDataIn = isKeyDataIn();
        if (isKeyDataIn && this.separator != null && this.splitKey != 0) {
            // 如果数据中有区分标识，则拆分并判断是否为需要处理的数据。
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
        
        // 实际子类需要实现的方法。
        map0(value, this.context);
    }
    
    /**
     * Called once at the end of the task.
     */
    protected void cleanup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {
        cleanup0(this.context);
    }
    
    /**
     * Called once at the start of the task.
     */
    public void setup0(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, KEYOUT, VALUEOUT>.Context context) throws IOException, InterruptedException {
        // NOTHING
    }
    
    @Override
    public void map0(String value, Mapper<LongWritable, Text, KEYOUT, VALUEOUT>.Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        
    }

    @Override
    protected boolean isKeyDataIn() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    protected boolean isKeyDataOut() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getOutputSplitKey() {
        // TODO Auto-generated method stub
        return 0;
    }

}
