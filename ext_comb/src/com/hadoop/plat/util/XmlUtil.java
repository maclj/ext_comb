package com.hadoop.plat.util;

import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * JAXB工具类。
 * 
 *
 */
public class XmlUtil {
	
	private static Log log = LogFactory.getLog(XmlUtil.class);

	/**
	 * 将对象转换为XML文档。 返回null，则错误。
	 * 
	 * @param src
	 * @param clazz
	 * @return
	 */
	public static String marshal(Object src, Class<?> clazz) {
		return marshal(src, clazz, true);
	}
	
	/**
	 * 将对象转换为XML文档。 返回null，则错误。
	 * 
	 * @param src
	 * @param clazz
	 * @return
	 */
	public static String marshal(Object src, Class<?> clazz, boolean format) {
		try {
			StringWriter sw = new StringWriter();
			JAXBContext ctx = JAXBContext.newInstance(clazz);
			Marshaller m = ctx.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, format);
			m.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
			m.marshal(src, sw);
			return sw.toString();

		} catch (JAXBException e) {
			log.warn(null, e);
		}
		return null;
	}
	
	/**
	 * 将XML转换为对象，注意传入的字符串必须已经是UTF-8编码。 返回null，则错误。
	 * 
	 * @param src
	 * @param clazz
	 * @return
	 */
	public static Object unmarshal(String src, Class<?> clazz) {
		try {
			JAXBContext ctx = JAXBContext.newInstance(clazz);
			Unmarshaller um = ctx.createUnmarshaller();
			return um.unmarshal(new StringReader(src));
		} catch (JAXBException e) {
			log.warn(null, e);
		}
		return null;
	}
	
	/**
	 * 将XML转换为对象，注意传入的字符串必须已经是UTF-8编码。 返回null，则错误。
	 * 
	 * @param src
	 * @param clazz
	 * @return
	 */
	public static Object unmarshal(File src, Class<?> clazz) {
		try {
			JAXBContext ctx = JAXBContext.newInstance(clazz);
			Unmarshaller um = ctx.createUnmarshaller();
			return um.unmarshal(src);
		} catch (JAXBException e) {
			log.warn(null, e);
		}
		return null;
	}
}
