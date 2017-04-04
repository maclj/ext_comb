package com.hadoop.util;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;


/**
 * 此类仅用于测试类装载，不要在项目中使用。
 * 服务器的类装载器，直接集成自URLClassLoader，使其具备从本地文件系统或
 * 远程 URL 进行类装载的能力。
 * 
 * 
 */
public class StandardClassLoader extends URLClassLoader {

    /**
     * 从某个URL集合中生成新的类装载器。
     * @param repositories URL集合
     */
    public StandardClassLoader(URL[] repositories) {
        super(repositories);
        System.out.println(toString());
    }

    /**
     * 从某个URL集合中生成新的类装载器，同时制定父类装载器。
     * @param repositories URL集合
     * @param parent 父类装载器
     */
    public StandardClassLoader(URL[] repositories, ClassLoader parent) {
        super(repositories, parent);
        System.out.println(toString());
    }

    /** {@inheritDoc} */
    public String getLoadedURLs() {
        return Arrays.toString(getURLs());
    }
    
    public String toString() {
        return "StandardClassLoader: " + getLoadedURLs();
    }
}
