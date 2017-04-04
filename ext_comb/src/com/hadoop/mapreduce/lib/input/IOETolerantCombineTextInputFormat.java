package com.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoop.entry.Jobs;

/**
 * 在读取InputSplit遇到IOE时记录日志，并跳过错误的InputSplit，保护Mapper正常执行。
 * 
 * @since 1.0
 */

public class IOETolerantCombineTextInputFormat extends CombineFileInputFormat<LongWritable, Text> {
    
    /** 重定向日志路径  */
	public static final String OUTPUT_IOE_PREFIX = "ioe/";

	/** LOG */
	private static final Log LOG = LogFactory.getLog(IOETolerantCombineTextInputFormat.class);
	
	/** 缺省的异常处理实现  */
    private static final LoggerIOELogHelper LOG4J_IOE_WRITER = new LoggerIOELogHelper();

	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		return new CombineFileRecordReader<LongWritable, Text>((CombineFileSplit) split, context,
				IOETolerantCombineTextRecordReader.class);
	}

	/**
	 * 生出输出目录，具体为"Job输出目录/输入文件相对JOB输入的路径"
	 * @param inputPath
	 * @param context
	 * @return
	 * @since  1.0
	 */
	public static String getOutputPath(Path inputFilePath, TaskAttemptContext context) {
	    Path[] jobInputPathes = FileInputFormat.getInputPaths(context);
	    String selectedInputPath = null;
	    String strInputFilePath = inputFilePath.toString();
	    for (Path inputPath : jobInputPathes){
	        String strInputPath = inputPath.toString();
	        if (strInputFilePath.startsWith(strInputPath)){
	            selectedInputPath = strInputPath;
	            break;
	        }
	    }
	    String jobOutputPath = FileOutputFormat.getOutputPath(context).toString();
	    String suffix = strInputFilePath == null ? strInputFilePath : strInputFilePath.substring(selectedInputPath.length());
	    return jobOutputPath + "/" + OUTPUT_IOE_PREFIX + suffix;
	}

	/**
	 * 参考CombineTextInputFormat.TextInputReader的实现,
	 * 可作为CombineFileRecordReader的输入参数。<br/>
	 *
	 * @see CombineFileRecordReader
	 * @see CombineFileInputFormat
	 * @see TextInputFormat
	 */
	public static class IOETolerantCombineTextRecordReader extends RecordReader<LongWritable, Text> {
		/**
		 * CombineFileRecordReaderWrapper的实现，参考CombineTextInputFormat.
		 * TextInputReader的实现
		 */
		private CombineFileRecordReaderWrapper<LongWritable, Text> readerWrapper;
		/** 输入文件 */
		private Path inputFile;
		/** 记录日志 */
		private LoggerIOELogHelper helper;
		/** 是否遇到IOException */
		private boolean ioexception = false;

		/**
		 * 构造函数，在CombineFileRecordReader每次更换输入文件时，调用这个签名的构造函数。
		 * 
		 * @param split
		 * @param context
		 * @param idx
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public IOETolerantCombineTextRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer idx)
				throws IOException, InterruptedException {
			readerWrapper = new CombineFileRecordReaderWrapper(new TextInputFormat(), split, context, idx) {
			};
			inputFile = split.getPath(idx);
		}

		/**
		 * 初始化，在构造后调用该方法。
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			readerWrapper.initialize(split, context);
			// 是否在命令行指定重定向IOE异常信息
			boolean outputIOE = Jobs.isEnvLogRedirect(context.getConfiguration());
			if (outputIOE && context instanceof TaskInputOutputContext) {
				this.helper = HDFSIOEWriteHelper.create((TaskInputOutputContext) context);
			} else {
				this.helper = LOG4J_IOE_WRITER;
			}
			this.helper.init(context);
		}

		/**
		 * 第一次遇到IOException时，通过日志记录错误信息，后续
		 */
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			try {
				return readerWrapper.nextKeyValue();
			} catch (IOException ioe) {
				ioexception = true;
				if (!helper.writeIOException(inputFile, ioe, OUTPUT_IOE_PREFIX)) {
					helper = LOG4J_IOE_WRITER;
				}
				return false;
			}
		}

		/**
		 * 遇到IOException后，返回FAILED_LONG，否则调用代理的readerWrapper
		 */
		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return readerWrapper.getCurrentKey();
		}

		/**
		 * 遇到IOException后，返回errorValue，否则调用代理的readerWrapper
		 */
		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return readerWrapper.getCurrentValue();
		}

		/**
		 * 遇到IOException后，返回1，否则调用代理的readerWrapper
		 */
		@Override
		public float getProgress() throws IOException, InterruptedException {
			return ioexception ? 1f : readerWrapper.getProgress();
		}

		/**
		 * 关闭Reader
		 */
		@Override
		public void close() throws IOException {
			if (readerWrapper != null) {
				try {
					readerWrapper.close();
				} finally {
					readerWrapper = null;
				}
			}
			if (helper != null) {
				try {
					helper.close();
				} catch (InterruptedException e) {
					LOG.warn("Close IOE Log Helper Failed.", e);
				} finally {
					helper = null;
				}
			}
		}
	}

	static class LoggerIOELogHelper {

		void init(final TaskAttemptContext context) throws IOException, InterruptedException {
		}

		boolean writeIOException(final Path inputFile, final IOException ioe, final String outputPath)
				throws IOException, InterruptedException {
			String log = getLog(inputFile.toString(), ioe);
			writeConsoleAndLog4j(log);
			return true;
		}

		static String getLog(final String inputPath, final IOException ioe) {
		    
		    PrintWriter pw = null;
		    try {
		        StringWriter sw = new StringWriter();
	            pw = new PrintWriter(sw);
	            pw.format("[Warning]Failed to read file(%s), exception: %s\n", inputPath, ioe.getMessage());
	            ioe.printStackTrace(pw);
	            pw.flush();
	            String output = sw.toString();
	            return output;
		    } finally {
		        if(pw != null) {
		            pw.close();
		        }
		    }
		}

		static void writeConsoleAndLog4j(String log) {
			System.out.println(log);
			LOG.warn(log);
		}

		void close() throws IOException, InterruptedException {
		}
	}

	static class HDFSIOEWriteHelper<KEY, VALUE> extends LoggerIOELogHelper {
		TaskAttemptContext context;
		int outputPathLength;

		static <KEY, VALUE> HDFSIOEWriteHelper<KEY, VALUE> create(
				final TaskInputOutputContext<?, ?, KEY, VALUE> context) {
			return new HDFSIOEWriteHelper<KEY, VALUE>();
		}

		@Override
		void init(final TaskAttemptContext context) throws IOException, InterruptedException {
			this.context = context;
		}

		@Override
		boolean writeIOException(final Path inputFile, final IOException ioe, final String outputPrefix)
				throws IOException, InterruptedException {
			String inputPath = inputFile.toString();
			String log = getLog(inputPath, ioe);
			writeConsoleAndLog4j(log);
			
			FSDataOutputStream fsdos = null;
			try {
			    //TODO 添加和job一致的压缩
			    Configuration conf = context.getConfiguration();
			    FileSystem fs = FileSystem.get(conf);
			    Path outputPath = new Path(getOutputPath(inputFile, context) + ".txt");
			    fsdos = fs.create(outputPath);
			    fsdos.writeBytes(log);
				return true;
			} catch (IOException e) {
				LOG.warn("Can not initial MultipleOutputs.", e);
				return false;
			} finally {
			    if(fsdos != null) {
			        fsdos.close();
			    }
			}
		}

		@Override
		void close() throws IOException, InterruptedException {
		}
	}
}
