package cn.likegirl.hadoop;

import cn.likegirl.hadoop.utils.ConvertUtil;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Test01 {
	
	@Test
	public void test1() {
		System.out.println(111);
	}

	@Test
	public void test2(){
		System.out.println(new Date().getTime());
	}

	@Test
	public void test3() throws ParseException {
		// Tue Jan 22 16:04:32 CST 2019
//		System.out.println(String.valueOf(new Date()));
		SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
		System.out.println(sdf.parse("Tue Jan 22 16:04:32 CST 2019").getTime());
	}

}
