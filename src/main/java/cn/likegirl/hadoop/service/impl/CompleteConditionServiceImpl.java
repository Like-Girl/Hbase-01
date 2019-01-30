package cn.likegirl.hadoop.service.impl;

import cn.likegirl.hadoop.hbase.RowMapper;
import cn.likegirl.hadoop.model.TspCompleteCondition;
import cn.likegirl.hadoop.service.BaseService;
import cn.likegirl.hadoop.service.CompleteConditionService;
import cn.likegirl.hadoop.utils.ConvertUtil;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;

/**
 * @author LikeGirl
 * @version v1.0
 * @title: CompleteConditionServiceImpl
 * @description: TODO
 * @date 2019/1/18 16:18
 */
@Service
public class CompleteConditionServiceImpl extends BaseService<TspCompleteCondition> implements CompleteConditionService {

    public final String HBASE_COMPLETECONDITION_TABLENAME = "tc1";
    public final String HBASE_COMPLETECONDITION_FAMILY = "info";

    @Override
    public List<TspCompleteCondition> find(String vin, Date startDate, Date endDate) {
        Scan scan = new Scan();
        // LEFBMCHCXJT054226_1542991932
        String startRow = vin + "_" + startDate.getTime();
        String endRow = vin + "_" + endDate.getTime();
        scan.setStartRow(startRow.getBytes());
        scan.setStopRow(endRow.getBytes());
        Field[] fields = TspCompleteCondition.class.getDeclaredFields();
        List<TspCompleteCondition> data = hbaseTemplate.find(HBASE_COMPLETECONDITION_TABLENAME, scan, new RowMapper<TspCompleteCondition>() {
            @Override
            public TspCompleteCondition mapRow(Result result, int i) throws Exception {
                TspCompleteCondition tc = new TspCompleteCondition();
                Field tcFile;
                for (Field field : fields) {
                    field.setAccessible(true);
                    String name = field.getName();
                    String value = Bytes.toString(result.getValue(HBASE_COMPLETECONDITION_FAMILY.getBytes(), name.getBytes()));
                    tcFile = tc.getClass().getDeclaredField(name);
                    if (tcFile != null) {
                        tcFile.setAccessible(true);
                        Class<?> fileType = tcFile.getType();
                        if (value != null) {
                            ConvertUtil.registerDateConvert();
                            tcFile.set(tc, ConvertUtil.convert(value, fileType));
                        }
                    }
                }
                return tc;
            }
        });
        return data;
    }

    @Override
    public List<TspCompleteCondition> select(String vin, Date startDate, Date endDate) {
        String startRow = vin + "_" + startDate.getTime();
        String endRow = vin + "_" + endDate.getTime();
        return select1(HBASE_COMPLETECONDITION_TABLENAME, HBASE_COMPLETECONDITION_FAMILY, startRow, endRow, null);
    }
}
