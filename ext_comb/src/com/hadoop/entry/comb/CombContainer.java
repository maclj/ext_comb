package com.hadoop.entry.comb;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 用在Container（mapper、reducer上的）的管理子类的定义。
 * 
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({java.lang.annotation.ElementType.TYPE})
public @interface CombContainer {

    String name() default "";
    
    String[] values() default "";
    
    Class<?>[] clazzes() default Object.class;
}
