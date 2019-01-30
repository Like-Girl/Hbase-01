package cn.likegirl.hadoop;

import cn.likegirl.hadoop.hbase.HbaseTemplate;
import cn.likegirl.hadoop.hbase.RowMapper;
import cn.likegirl.hadoop.hbase.TableCallback;
import cn.likegirl.hadoop.model.TspCompleteCondition;
import cn.likegirl.hadoop.service.CompleteConditionService;
import cn.likegirl.hadoop.utils.ConvertUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.lang.reflect.Field;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class Hbase01ApplicationTests {

	@Autowired
    HbaseTemplate hbaseTemplate;

    @Test
    public void find01(){
        Scan scan = new Scan();
        scan.setStartRow("LEFBMCHCXJT054226_1542967066".getBytes());
        scan.setStopRow("LEFBMCHCXJT054226_1542991932".getBytes());
        Field[] fields = TspCompleteCondition.class.getDeclaredFields();
        List<TspCompleteCondition> data = hbaseTemplate.find("tc", scan, new RowMapper<TspCompleteCondition>() {
            @Override
            public TspCompleteCondition mapRow(Result result, int i) throws Exception {
                System.err.println(result.toString());
                TspCompleteCondition tc = new TspCompleteCondition();
                Field tcFile;
                for(Field field : fields){
                    field.setAccessible(true);
                    String name = field.getName();
                    String value = Bytes.toString(result.getValue("info".getBytes(), name.getBytes()));
                    tcFile = tc.getClass().getDeclaredField(name);
                    if(tcFile != null){
                        tcFile.setAccessible(true);
                        Class<?> fileType = tcFile.getType();
                        if(value != null){
                            ConvertUtil.registerDateConvert();
                            tcFile.set(tc,ConvertUtil.convert(value,fileType));
                        }
                    }
                }
                return tc;
            }
        });
        System.out.println(data.size());
    }

    @Autowired
    CompleteConditionService completeConditionService;

    @Autowired
    Configuration configuration;

    @Test
    public void find02() throws ParseException {
        //LEFBMCHCXJT054226_1542991932
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<TspCompleteCondition> lefbmchcxjt054226 = completeConditionService.find("LEFBMCHCXJT054226", format.parse("2018-12-23 10:23:00"), format.parse("2018-12-23 11:23:00"));
        System.out.println(lefbmchcxjt054226.size());
    }

    @Test
    public void find03() throws ParseException {
        //LEFBMCHCXJT054226_1542991932
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<TspCompleteCondition> lefbmchcxjt054226 = completeConditionService.select("LEFBMCHCXJT054226", format.parse("2018-12-23 10:23:00"), format.parse("2018-12-23 11:23:00"));
        System.out.println(lefbmchcxjt054226.size());
    }

    @Test
    public void find04(){
        hbaseTemplate.execute("tc", new TableCallback<Object>() {
            @Override
            public Object doInTable(HTableInterface hTableInterface) throws Throwable {
//                Get get = new Get();
//                hTableInterface.get(get);
                return null;
            }
        });
    }


}

