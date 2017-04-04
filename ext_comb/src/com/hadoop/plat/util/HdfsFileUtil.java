package com.hadoop.plat.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * HDFS 文件操作工具类
 * 
 * 
 *
 */
public class HdfsFileUtil {

    /**
     * 判断文件夹是否存在。
     * 
     * @param conf
     * @param dir
     */
    public static boolean isDirectoryExist(Configuration conf, String dir) {
        try {
            
            FileSystem fs = FileSystem.get(conf);
            if (fs == null) {
                return false;
            }
            FileStatus fstatus = fs.getFileStatus(new Path(dir));
            if (fstatus == null) {
                return false;
            }
            if (fstatus.isDirectory()) {
                return true;
            }
        } catch (Exception e) {
            // ignored
        }
        return false;
    }
    
    /**
     * 转换为Linux系统下的分隔符号，并且在最末端加上/。
     * @param src
     * @return
     */
    public static String normalize(String src) {
        src = src.replaceAll("\\\\", "/");
        if(src.lastIndexOf("/") == src.length() -1) {
            return src;
        }
        return src + "/";
    }
    
    public static InputStream open(Configuration conf, CompressionCodec codec, String hdfsPath, InputStream in) {
        
        InputStream stream = null;
        if (conf != null && hdfsPath != null) {
            stream = getHdfs(hdfsPath, conf, codec);
            if (stream != null) {
                return stream;
            }
        }
		stream = getLocal(in);
		return stream;
	}
	
	public static InputStream open(Configuration conf, String hdfsPath, InputStream in) {
		return open(conf, ReflectionUtils.newInstance(GzipCodec.class, conf), hdfsPath, in);
	}

	private static InputStream getHdfs(String hdfsPath, Configuration conf, CompressionCodec codec) {

		InputStream stream = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
			FSDataInputStream fsstream = fs.open(new Path(hdfsPath));
			if (fsstream == null) {
				return stream;
			}
			
			CompressionCodec tmp = codec;
			if (tmp == null) {
				CompressionCodecFactory factory = new CompressionCodecFactory(conf);
				tmp = factory.getCodec(new Path(hdfsPath));
			}
			stream = tmp.createInputStream(fsstream);
			
		} catch (Exception e) {
			// ignored
		}
		return stream;
	}

	private static InputStream getLocal(InputStream in) {
		InputStream stream = null;
		try {
			stream = new GZIPInputStream(in);
		} catch (Exception e) {
			// ignored
		}
		return stream;
	}
	
	@SuppressWarnings("resource")
	@Deprecated
	public static void write(InputStream in, CompressionCodec codec, Configuration conf, String hdfsPath, String localPath){
		if (StringUtils.isBlank(hdfsPath) && StringUtils.isBlank(localPath)){
			return;
		}
		OutputStream out = null;
		if (StringUtils.isNotBlank(hdfsPath)){
			try {
				FileSystem fs = FileSystem.get(conf);
				FSDataOutputStream fsout = fs.create(new Path(hdfsPath));
				out = fsout;                   
				if (codec != null){
					out = codec.createOutputStream(fsout);
				}
				byte[] buf = new byte[1024];
				while (in.read(buf) != -1){
					out.write(buf);
				}
				out.flush();
			} catch (Exception e) {   
				// ignored
			} finally {
				IOUtils.closeQuietly(in);
				IOUtils.closeQuietly(out);
			}
			return;
		}
		
		try {
			FileOutputStream fout = new FileOutputStream(new File(localPath));
			out = fout;
			if (codec != null){
				out = codec.createOutputStream(fout);
			}
			byte[] buf = new byte[1024];
			while (in.read(buf) != -1){
				out.write(buf);
			}
			out.flush();
		} catch (Exception e){
			// ignored
		} finally {
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(out);
		}
	}
	
	@Deprecated
	public static void writeToHdfs(InputStream in, CompressionCodec codec, Configuration conf, String hdfsPath){
		write(in, codec, conf, hdfsPath, null);
	}
	
	@Deprecated
    public static void writeToLocal(InputStream in, CompressionCodec codec, Configuration conf, String localPath){
		write(in, codec, conf, null, localPath);
	}
    
	@Deprecated
    public static void writeToHdfs(InputStream in, Configuration conf, String hdfsPath){
		writeToHdfs(in, ReflectionUtils.newInstance(GzipCodec.class, conf), conf, hdfsPath);
	}
	
	@Deprecated
    public static void writeToLocal(InputStream in, Configuration conf, String localPath){
		writeToLocal(in, ReflectionUtils.newInstance(GzipCodec.class, conf), conf, localPath);
	}
}
