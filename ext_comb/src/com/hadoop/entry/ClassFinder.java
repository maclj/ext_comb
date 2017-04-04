package com.hadoop.entry;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import com.hadoop.util.Logger;


/**
 * 在当前类装载器的范围内检索指定条件的类。<br>
 * 注意当前加载类所属的jar以及当前classloader加载的其它jar。<br>
 * 
 * RunJar() -> 加载 临时目录下解压出来的 class、classes目录下的class、lib目录下的jar<br>
 * GenericOptionsParser() -> 加载 tmpjars中的jar，并将前一个设置为parent<br>
 * 
 * 我们只需要取到当前的class loader，并从中检索所有的jar和classes文件即可。<br>
 * 
 * 
 * @version 1.0.0
 */
public class ClassFinder {

    /** URL prefix for loading from the file system: "file:" */
    public static final String FILE_URL_PREFIX = "file:";

    /** URL prefix for loading from the file system: "jar:" */
    public static final String JAR_URL_PREFIX = "jar:";

    /** URL protocol for a file in the file system: "file" */
    public static final String URL_PROTOCOL_FILE = "file";

    /** URL protocol for an entry from a jar file: "jar" */
    public static final String URL_PROTOCOL_JAR = "jar";

    /** URL protocol for an entry from a zip file: "zip" */
    public static final String URL_PROTOCOL_ZIP = "zip";

    /** File extension for a regular jar file: ".jar" */
    public static final String JAR_FILE_EXTENSION = ".jar";
    
    /** File extension for a regular class file: ".class" */
    public static final String CLASS_FILE_EXTENSION = ".class";

    /** Separator between JAR URL and file path within the JAR: "!/" */
    public static final String JAR_URL_SEPARATOR = "!/";

    /**
     * Determine whether the given URL points to a jar file itself,
     * that is, has protocol "file" and ends with the ".jar" extension.
     * @param url the URL to check
     * @return whether the URL has been identified as a JAR file URL
     */
    public static boolean isJarFileURL(URL url) {
        return (URL_PROTOCOL_FILE.equals(url.getProtocol()) &&
                url.getPath().toLowerCase().endsWith(JAR_FILE_EXTENSION));
    }
    
    /**
     * Determine whether the given URL points to a class file itself,
     * that is, has protocol "file" and ends with the ".class" extension.
     * @param url the URL to check
     * @return whether the URL has been identified as a JAR file URL
     */
    public static boolean isFileURL(URL url) {
        return (URL_PROTOCOL_FILE.equals(url.getProtocol()));
    }
    
    /**
     * Determine whether the given URL points to a resource in a jar file,
     * that is, has protocol "jar", "zip"
     * @param url the URL to check
     * @return whether the URL has been identified as a JAR URL
     */
    public static boolean isJarURL(URL url) {
        String protocol = url.getProtocol();
        return (URL_PROTOCOL_JAR.equals(protocol) || URL_PROTOCOL_ZIP.equals(protocol));
    }
    
    
    /**
     * Extract the URL for the actual jar file from the given URL
     * (which may point to a resource in a jar file or to a jar file itself).
     * @param jarUrl the original URL
     * @return the URL for the actual jar file
     * @throws MalformedURLException if no valid jar file URL could be extracted
     */
    public static URL extractJarFileURL(URL jarUrl) throws MalformedURLException {
        String urlFile = jarUrl.getFile();
        int separatorIndex = urlFile.indexOf(JAR_URL_SEPARATOR);
        if (separatorIndex != -1) {
            String jarFile = urlFile.substring(0, separatorIndex);
            try {
                return new URL(jarFile);
            }
            catch (MalformedURLException ex) {
                // Probably no protocol in original jar URL, like "jar:C:/mypath/myjar.jar".
                // This usually indicates that the jar file resides in the file system.
                if (!jarFile.startsWith("/")) {
                    jarFile = "/" + jarFile;
                }
                return new URL(FILE_URL_PREFIX + jarFile);
            }
        }
        else {
            return jarUrl;
        }
    }
    
    /**
     * Return the default ClassLoader to use: typically the thread context
     * ClassLoader, if available; the ClassLoader that loaded the ClassUtils
     * class will be used as fallback.
     * <p>Call this method if you intend to use the thread context ClassLoader
     * in a scenario where you clearly prefer a non-null ClassLoader reference:
     * for example, for class path resource loading (but not necessarily for
     * {@code Class.forName}, which accepts a {@code null} ClassLoader
     * reference as well).
     * @return the default ClassLoader (only {@code null} if even the system
     * ClassLoader isn't accessible)
     * @see Thread#getContextClassLoader()
     * @see ClassLoader#getSystemClassLoader()
     */
    public static ClassLoader getDefaultClassLoader() {
        ClassLoader cl = null;
        try {
            cl = Thread.currentThread().getContextClassLoader();
        }
        catch (Throwable ex) {
            // Cannot access thread context ClassLoader - falling back...
        }
        if (cl == null) {
            // No thread context class loader -> use class loader of this class.
            cl = ClassFinder.class.getClassLoader();
        }
        
        if (cl == null) {
            // getClassLoader() returning null indicates the bootstrap ClassLoader
            try {
                cl = ClassLoader.getSystemClassLoader();
            }
            catch (Throwable ex) {
                // Cannot access system ClassLoader - oh well, maybe the caller can live with null...
            }
        }
        if(cl == null) {
            throw new RuntimeException("can't get the classloader.");
        }
        return cl;
    }
    
    /**
     * Find all class location resources with the given path via the ClassLoader.
     * Called by {@link #findAllClassPathResources(String)}.
     * @param path the absolute path within the classpath (never a leading slash)
     * @return a mutable Set of matching Resource instances
     */
    private static Set<URL> doFindAllClassPathResources(ClassLoader loader, String path, boolean findParent) throws IOException {
        Set<URL> result = new LinkedHashSet<URL>(16);
        // 当前路径下的classes或者jar
        Enumeration<URL> resourceUrls = loader.getResources(path);
        while (resourceUrls.hasMoreElements()) {
            URL url = resourceUrls.nextElement();
            result.add(url);
        }
        // The above result is likely to be incomplete, i.e. only containing file system references.
        // We need to have pointers to each of the jar files on the classpath as well...
        // other jars in class loader
        addAllClassLoaderJarRoots(loader, result, findParent);
        return result;
    }
    
    /**
     * Search all {@link URLClassLoader} URLs for jar file references and add them to the
     * given set of resources in the form of pointers to the root of the jar file content.
     * @param classLoader the ClassLoader to search (including its ancestors)
     * @param result the set of resources to add jar roots to
     * @param findParent
     */
    private static void addAllClassLoaderJarRoots(ClassLoader classLoader, Set<URL> result, boolean findParent) {

        if (classLoader instanceof URLClassLoader) {
            try {
                for (URL url : ((URLClassLoader) classLoader).getURLs()) {
                    if (isJarFileURL(url)) {
                        result.add(url);
                    }
                }
            } catch (Exception ex) {
                log("Cannot introspect jar files since ClassLoader [" + classLoader + "] does not support 'getURLs()': "
                        + ex);
            }
        }

        // 递归找parent
        if (classLoader != null && findParent) {
            try {
                addAllClassLoaderJarRoots(classLoader.getParent(), result, findParent);
            } catch (Exception ex) {
                log("Cannot introspect jar files in parent ClassLoader since [" + classLoader
                        + "] does not support 'getParent()': " + ex);
            }
        }
    }
    
    
    /**
     * 根据包名检索
     * @param ClassLoader loader
     * @param packName 包名
     * @return
     */
    public static Set<Class<?>> findClass(String packName) {
        ClassLoader loader = getDefaultClassLoader();
        return findClass(loader, packName);
    }
    
    /**
     * 根据包名检索
     * @param ClassLoader loader
     * @param packName 包名
     * @return
     */
    public static Set<Class<?>> findClass(ClassLoader loader, String packName) {
        return findClass(loader, packName, false);
    }

    /**
     * 根据包名检索
     * @param ClassLoader loader
     * @param packName 包名
     * @parem findParent 查找parent
     * @return
     */
    public static Set<Class<?>> findClass(ClassLoader loader, String packName, boolean findParent) {
        Set<Class<?>> classes = new LinkedHashSet<Class<?>>();
        boolean flag = true;// 是否递归

        String packDir = packName.replace(".", "/");
        // System.out.println("packDir: " + packDir);
        
        try {
            Set<URL> urls = doFindAllClassPathResources(loader, packDir, findParent);
            
            for(URL url : urls) {
                // System.out.println("ext: " + url.toString());
                // note: 需要保持判断顺序，优先处理jar && file 的情况。
                if(isJarFileURL(url)) {
                    // other jars in class loader
                    String filePath = URLDecoder.decode(url.getFile(), "UTF-8");
                    log("find jarFile: " + filePath);
                    JarFile jar = new JarFile(filePath);
                    readClassInJar(jar, classes, loader, packName);
                } else if(isFileURL(url)) {
                    // current classes
                    String filePath = URLDecoder.decode(url.getFile(), "UTF-8");
                    log("find classFile: " + filePath);
                    // System.out.println("filePath :" + filePath);
                    findAndAddClassesInPackageByFile(loader, packName, filePath, flag, classes);
                } else if(isJarURL(url)) {
                    // current jar
                    JarFile jar = ((JarURLConnection) url.openConnection()).getJarFile();
                    log("find jarURL: " + jar.getName());
                    readClassInJar(jar, classes, loader, packName);
                } 
            }

        } catch (IOException e) {
            // ignored
        }
        return classes;
    }
    
    /**
     * find class in jar file.
     * @param jar
     * @param classes
     * @param loader
     * @param packName
     */
    private static void readClassInJar(JarFile jar, Set<Class<?>> classes, ClassLoader loader, String packName) {

        if (jar == null) {
            return;
        }
        Enumeration<JarEntry> entries = jar.entries();
        while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            String name = entry.getName();
            if (name == null || !name.endsWith(".class")) {
                continue;
            }
            name = name.replace("/", ".").substring(0, name.length() - 6);
            if (packName != null && packName != "") {
                if (!name.startsWith(packName)) {
                    continue;
                }
            }
            try {
                classes.add(loader.loadClass(name));
            } catch (NoClassDefFoundError e) {
                // ignored
            } catch (ClassNotFoundException e) {
                // ignored
            }
            // ......
        }
        try {
            jar.close();
        } catch (Exception e) {
            // ignored
        }
    }
    
    
    /**
     * 在指定包内查找符合指定annotation定义的类。
     * @param packName
     * @param annotationClass
     * @return
     */
    public static Set<Class<?>> findClass(String packName, Class<? extends Annotation> annotationClass) {

        Set<Class<?>> classes = findClass(packName);
        Iterator<Class<?>> ite = classes.iterator();
        Class<?> clazz = null;
        while (ite.hasNext()) {
            clazz = ite.next();
            if (!clazz.isAnnotationPresent(annotationClass)) {
                ite.remove();
            }
        }
        return classes;
    }
    
    /**
     * 在目录中递归搜索class，针对未打成jar的情况。
     * @param loader ClassLoader
     * @param packName 包名
     * @param filePath 路径
     * @param flag 递归标识
     * @param classes 结果集合
     */
    private static void findAndAddClassesInPackageByFile(ClassLoader loader, String packName, String filePath, final boolean flag,
            Set<Class<?>> classes) {
        File dir = new File(filePath);
        // 目录不存在
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }
        File[] dirfiles = dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return flag && pathname.isDirectory() || pathname.getName().endsWith(CLASS_FILE_EXTENSION);
            }
        });
        
        if(dirfiles == null) {
            return;
        }
        for (File file : dirfiles) {
            // 如果是目录，递归
            if (file.isDirectory()) {
                findAndAddClassesInPackageByFile(loader, packName + "." + file.getName(), file.getAbsolutePath(), flag,
                        classes);
            } else {
                // 如果是文件
                String className = file.getName().substring(0, file.getName().length() - 6);
                try {
                    classes.add(loader.loadClass(packName + "." + className));
                } catch (ClassNotFoundException e) {
                    // ignored
                }
            }
        }
    }
    
    /**
     * 在指定包内查找符合指定JobConf定义且工程名称匹配的类。
     * @param packName packName,可以以逗号分隔多个包名
     * @param annotationClass
     * @return
     */
    public static List<JobDefine> findClassByJobConf(ClassLoader loader, String packName, String project, int index) {

        List<JobDefine> list = new LinkedList<JobDefine>();
        String[] packes = packName.split(",");
        Set<Class<?>> classes = new HashSet<Class<?>>();
        for(String name : packes) {
            classes.addAll(findClass(loader, name));
        }
        
        log("find classes: " + classes.size());
        
        String[] projects = project.split(",");
        log("find projects: " + Arrays.toString(projects) );
        
        // 按照配置中的项目顺序
        for(String proj : projects) {
            list.addAll(findClassByJobConf(classes, proj, index) );
        }
        return list;
    }
    
    /**
     * 在指定范围内查找项目对应的实现。
     * @param classes
     * @param project
     * @param index
     * @return
     */
    private static List<JobDefine> findClassByJobConf(Set<Class<?>> classes, String project, int index) {
        
        List<JobDefine> list = new LinkedList<JobDefine>();
        
        Iterator<Class<?>> ite = classes.iterator();
        Class<?> clazz = null;
        JobDefine jd = null;
        while (ite.hasNext()) {
            clazz = ite.next();
            jd = clazz.getAnnotation(JobDefine.class);
            if (jd == null) {
                continue;
            }
            // 不是该项目
            if(!jd.project().equals(project) ) {
                continue;
            }
            
            // index 大于0，且jobSeq小于该值时加入运行
            if(index == 0) {
                list.add(jd);
            } else if(jd.jobSeq() < index) {
                list.add(jd);
            } else {
                // not implement
            }
        }
        Collections.sort(list, new Comparator<JobDefine>() {
            @Override
            public int compare(JobDefine o1, JobDefine o2) {
                return o1.jobSeq() - o2.jobSeq();
            }
        });
        return list;
    }
    
    public static List<JobDefine> findClassByJobConf(ClassLoader loader, String packName, String project){
        return findClassByJobConf(loader, packName, project, 0);
    }
    
    public static List<JobDefine> findClassByJobConf(String packName, String project){
        ClassLoader loader = getDefaultClassLoader();
        // log("loader = " + loader);
        return findClassByJobConf(loader, packName, project, 0);
    }
    
    /**
     * 
     * @param packName
     * @param project
     * @return
     */
    public static List<Project> findProjectsByJobConf(String packName, String project) {
        ClassLoader loader = getDefaultClassLoader();
        return findProjectsByJobConf(loader, packName, project, 0);
    }
    
    
    /**
     * 在指定包内查找符合指定JobConf定义且工程名称匹配的类。
     * @param packName packName,可以以逗号分隔多个包名
     * @param annotationClass
     * @return
     */
    public static List<Project> findProjectsByJobConf(ClassLoader loader, String packName, String project, int index) {

        List<Project> list = new LinkedList<Project>();
        
        String[] packes = packName.split(",");
        Set<Class<?>> classes = new HashSet<Class<?>>();
        for(String name : packes) {
            classes.addAll(findClass(loader, name));
        }
        
        log("find classes: " + classes.size());
        
        String[] projects = project.split(",");
        log("find projects: " + Arrays.toString(projects) );
        
        // 按照配置中的项目顺序
        for(String proj : projects) {
            list.add(findProjectsByJobConf(classes, proj, index) );
        }
        return list;
    }
    
    /**
     * 在指定范围内查找项目对应的实现。
     * @param classes
     * @param project
     * @param index
     * @return
     */
    private static Project findProjectsByJobConf(Set<Class<?>> classes, String project, int index) {
        
        List<JobDefine> list = findClassByJobConf(classes, project, index);
        return new Project(project, list);
    }
    
    /**
     * 在指定包下查找存在对应annotation定义的类。
     * @param packName
     * @param annotations
     * @return
     */
    public static Set<Class<?>> findClassByAnnotation(String packName, Class<? extends Annotation> annotation) {
        
        Set<Class<?>> classes = new HashSet<Class<?>>();
        if(annotation == null) {
            return classes;
        }
        
        ClassLoader loader = getDefaultClassLoader();
        String[] packes = packName.split(",");
        for(String name : packes) {
            classes.addAll(findClass(loader, name));
        }
        
        log("find classes(" + annotation.getName() + "): " + classes.size());
        
        Iterator<Class<?>> ite = classes.iterator();
        Class<?> clazz = null;
        while (ite.hasNext()) {
            clazz = ite.next();
            if (!hasAnnotation(clazz, annotation)) {
                ite.remove();
                continue;
            }
        }
        return classes;
    }
    
    /**
     * 只要能找到其中一个Annotation，则认为是成功。
     * @param clazz
     * @param annotations
     * @return
     */
    private static boolean hasAnnotation(Class<?> clazz, Class<? extends Annotation> annotation) {
        if(clazz == null) {
            return false;
        }
        
        if(clazz.getAnnotation(annotation) != null) {
            return true;
        }
        
//        for(Class<? extends Annotation> ano : annotations) {
//            
//        }
        return false;
    }
    
    
    /**
     * 根据类名找到指定的class对象，
     * @param packName
     * @param clazzNames
     * @return
     */
    public static List<Class<?>> findClassByPackAndNames(String packName, String[] clazzNames) {
        
        List<Class<?>> result = new ArrayList<Class<?>>();
        if(packName == null || packName.trim().length() == 0) {
            return result;
        }
        if(clazzNames == null || clazzNames.length == 0) {
            return result;
        }
        ClassLoader loader = ClassFinder.getDefaultClassLoader();

        String[] packes = packName.split(",");
        Map<String, Class<?>> clazzMap = new HashMap<String, Class<?>>();
        Set<Class<?>> classes = null;
        for(String name : packes) {
            classes = ClassFinder.findClass(loader, name, false);//目前不需要递归找
            if(classes.size() == 0) {
                continue;
            }
            for(Class<?> clazz : classes) {
                clazzMap.put(clazz.getName(), clazz);
                Logger.print(clazz.getName());
            }
        }
        // System.out.println(clazzMap.keySet());
        Class<?> value = null;
        for(String name : clazzNames) {
            value = clazzMap.get(name);
            if(value == null) {
                name = toInnerClassName(name);
                value = clazzMap.get(name);
                if(value == null) {
                    continue;
                }
            }
            result.add(value);
        }
        if(result.size() != clazzNames.length) {
        }
        classes = null;
        clazzMap = null;
        return result;
    }
    
    /**
     * 获取对应内部类的实际名称。
     * @param name
     * @return
     */
    public static String toInnerClassName(String name) {
        if(name == null) {
            return null;
        }
        
        int index = name.lastIndexOf(".");
        if(index <=0 || index == name.length()-1) {
            return null;
        }
        // 兼容一下写法，如果使用者没有按照内部类的方式定义
        name = name.substring(0, index) + "$" + name.substring(index + 1);
        return name;
    }
    
    /**
     * 根据名称构造对象。
     * @param clazzName
     * @return
     */
    public static Object newInstance(String clazzName) {
        Object obj = null;
        try {
            obj = Class.forName(clazzName).newInstance();
        } catch (Exception e) {
            Logger.warn(null, e);
        }
        if (obj == null) {
            // 兼容一下写法，如果使用者没有按照内部类的方式定义
            clazzName = toInnerClassName(clazzName);
            try {
                obj = Class.forName(clazzName).newInstance();
            } catch (Exception e) {
                Logger.warn(null, e);
            }
        }
        return obj;
    }
    
    /**
     * 根据名称构造对象。
     * @param clazzName
     * @return
     */
    public static Object newInstance(Class<?> clazz) {
        Object obj = null;
        try {
            obj = clazz.newInstance();
        } catch (Exception e) {
            Logger.warn(null, e);
        }
        
        return obj;
    }
    
    /**
     * 根据名称加载Class对象。
     * @param clazzName
     * @return
     */
    public static Class<?> loadClass(String clazzName) {
    	Class<?> obj = null;
        try {
            obj = Class.forName(clazzName);
        } catch (Exception e) {
            Logger.warn(null, e);
        }
        if (obj == null) {
            // 兼容一下写法，如果使用者没有按照内部类的方式定义
            clazzName = toInnerClassName(clazzName);
            try {
                obj = Class.forName(clazzName);
            } catch (Exception e) {
                Logger.warn(null, e);
            }
        }
        return obj;
    }
    
    /**
     * 在指定包内查找符合CombStage定义，且名称匹配的class。
     * @param packName packName,可以以逗号分隔多个包名
     * @param combStages ,可以以逗号分隔多个stage名称
     * @return Class<?>[] 顺序与combStages内容保持一致
     */
    @Deprecated
    public static Class<?>[] findClassByCombStage(String packName, String combStages) {

        if(combStages == null || combStages.length() == 0) {
            return null;
        }
        // 加载class
        ClassLoader loader = getDefaultClassLoader();
        String[] packes = packName.split(",");
        Set<Class<?>> classes = new HashSet<Class<?>>();
        for(String name : packes) {
            classes.addAll(findClass(loader, name));
        }

        // 检索combstage定义
        String[] stages = combStages.split(",");
        Class<?>[] result = new Class<?>[stages.length];
        
        Iterator<Class<?>> ite = classes.iterator();
        Class<?> clazz = null;
        com.hadoop.entry.comb.CombStage cs = null;
        int index = 0;
        while (ite.hasNext()) {
            clazz = ite.next();
            cs = clazz.getAnnotation(com.hadoop.entry.comb.CombStage.class);
            if (cs == null) {
                // ite.remove();
                continue;
            }
            // 不是该项目
            index = findIndex(stages, cs.name());
            if( index == stages.length ) {
                // ite.remove();
                continue;
            }
            // 如果代码中出现了重复，则取了最后一个。
            result[index] = clazz;
        }
        log("find classes(CombStage): " + result.length);
        return result;
    }
    
    private static int findIndex(String[] stages, String name) {
        int i = 0;
        for(String row : stages) {
            if(row.equals(name)) {
                return i;
            }
            i++;
        }
        return i;
    }

//    /**
//     * 获取属性对应的泛型定义。
//     * @param field
//     * @return
//     */
//    public static Class<?> getGenericType(Field field) {
//        Type t = field.getType();
//        Class<?> genericType = null;
//        if (ParameterizedType.class.isAssignableFrom(t.getClass())) {
//            Type[] types = ((ParameterizedType) t).getActualTypeArguments();
//            // genericType = (Class<?>) types[0];
//            genericType = types[0].getClass();
//            // System.out.println(genericType);
//        }
//        return genericType;
//    }
    
    private static void log(String str) {
        //Logger.info(str);
    }

    public static void main(String[] args) {
        Set<Class<?>> cs = ClassFinder.findClass("com.hadoop", JobDefine.class);
        for (Class<?> clazz : cs) {
            System.out.println("find:" + clazz.getCanonicalName());
        }
        
        String[] names = new String[]{"com.hadoop.mapreduce.ContainerKeyMapper"};
        List<Class<?>> clazzes = ClassFinder.findClassByPackAndNames("com.hadoop", names);
        System.out.println(clazzes);
    }

}
