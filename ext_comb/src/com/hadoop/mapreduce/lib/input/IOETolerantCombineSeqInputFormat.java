package com.hadoop.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineSequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import com.hadoop.util.Logger;

/**
 * 读取二进制文件，遇到IO异常不再读取
 * 
 *
 */
public class IOETolerantCombineSeqInputFormat extends CombineSequenceFileInputFormat<LongWritable, Text>{

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		return new CombineFileRecordReader<LongWritable, Text>((CombineFileSplit)split, context,
				IOETolerantCombineSeqRecordReader.class);
	}
	
	public static class IOETolerantCombineSeqRecordReader extends RecordReader<LongWritable, Text>{
		
		private CombineFileRecordReaderWrapper<LongWritable, Text> readerWrapper;
		
		private boolean ioexception = false;
		
		public IOETolerantCombineSeqRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer idx) throws IOException, InterruptedException{
			readerWrapper = new CombineFileRecordReaderWrapper<LongWritable, Text>(new SequenceFileInputFormat<LongWritable, Text>(),
					split, context, idx) {};			
		}

		@Override
		public void close() throws IOException {
			readerWrapper.close();
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return readerWrapper.getCurrentKey();
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return readerWrapper.getCurrentValue();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return ioexception ? 1f : readerWrapper.getProgress();
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			readerWrapper.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			try {
				return readerWrapper.nextKeyValue();
			} catch (Exception e){
				Logger.warn(null, e);
				return false;
			}
		}
		
	}

}
