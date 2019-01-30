package cn.likegirl.hadoop.utils;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.Converter;
import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * @author LikeGirl
 * @version v1.0
 * @title: ConvertUtil
 * @description: TODO
 * @date 2019/1/18 14:33
 */
public class ConvertUtil extends ConvertUtils {

    private static final String TIMESTAMP_FORMAT = "^\\d+$";

    private static final String DEFAULT_DATE_TIME_FORMAT = "^\\d{4}(\\-)\\d{1,2}\\1\\d{1,2} \\d{2}:\\d{2}:\\d{2}$";

    private static final String DATE_TIME_MS_FORMAT = "^\\d{4}(\\-)\\d{1,2}\\1\\d{1,2} \\d{2}:\\d{2}:\\d{2}.\\d+$";

    private static final String DEFAULT_DATE_FORMAT = "^\\d{4}(\\-)\\d{1,2}\\1\\d{1,2}";

    private static final String SLASH_DATE_TIME_FORMAT = "^\\d{4}(\\/)\\d{1,2}\\1\\d{1,2} \\d{2}:\\d{2}:\\d{2}$";

    private static final String SLASH_DATE_TIME_MS_FORMAT = "^\\d{4}(\\/)\\d{1,2}\\1\\d{1,2} \\d{2}:\\d{2}:\\d{2}.\\d+$";

    private static final String SLASH_DATE_FORMAT = "^\\d{4}(\\/)\\d{1,2}\\1\\d{1,2}$";

    static {
        registerDateConvert();
    }

    public static void registerDateConvert(){
        ConvertUtils.register(new Converter() {
            @SuppressWarnings("unchecked")
            @Override
            public <T> T convert(Class<T> type, Object value) {
                String val = Optional.ofNullable(value).map(String::valueOf).map(String::trim).orElse(null);
                if (StringUtils.isEmpty(val)) {
                    return null;
                }
                if(Pattern.matches(TIMESTAMP_FORMAT,val)){
                    return (T) new Date(Long.valueOf(val));
                }
                String dateFormat = null;
                if (Pattern.matches(DEFAULT_DATE_TIME_FORMAT, val)) {
                    dateFormat = "yyyy-MM-dd HH:mm:ss";
                }
                if (Pattern.matches(DEFAULT_DATE_FORMAT, val)) {
                    dateFormat = "yyyy-MM-dd";
                }
                if(Pattern.matches(DATE_TIME_MS_FORMAT, val)){
                    dateFormat = "yyyy-MM-dd HH:mm:ss.SSS";
                }
                if (Pattern.matches(SLASH_DATE_TIME_FORMAT, val)) {
                    dateFormat = "yyyy/MM/dd HH:mm:ss";
                }
                if (Pattern.matches(SLASH_DATE_FORMAT, val)) {
                    dateFormat = "yyyy-MM-dd";
                }
                if(Pattern.matches(SLASH_DATE_TIME_MS_FORMAT, val)){
                    dateFormat = "yyyy-MM-dd HH:mm:ss.SSS";
                }
                if(null == dateFormat){
                    throw new RuntimeException("[java.util.Date]类型转换时异常：参数格式错误!");
                }
                try {
                    SimpleDateFormat sd = new SimpleDateFormat(dateFormat);
                    return (T) sd.parse(val);
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        }, java.util.Date.class);
    }
}
