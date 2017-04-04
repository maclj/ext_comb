package com.hadoop.entry;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({java.lang.annotation.ElementType.TYPE})
public @interface JobComb {

    String project() default "";

    int seq() default 0;
    
    String key() default "##";
}
