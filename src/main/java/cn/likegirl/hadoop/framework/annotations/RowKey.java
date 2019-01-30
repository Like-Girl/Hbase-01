package cn.likegirl.hadoop.framework.annotations;

import java.lang.annotation.*;

/**
 * @author LikeGirl
 * @version v1.0
 * @title: RowKey
 * @description: TODO
 * @date 2019/1/22 16:27
 */
@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RowKey {

    String value() default "rowKey";
}
