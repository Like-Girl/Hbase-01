package cn.likegirl.hadoop.service;

import cn.likegirl.hadoop.framework.annotations.RowKey;
import cn.likegirl.hadoop.hbase.HbaseTemplate;
import cn.likegirl.hadoop.hbase.ResultsExtractor;
import cn.likegirl.hadoop.hbase.RowMapper;
import cn.likegirl.hadoop.hbase.TableCallback;
import cn.likegirl.hadoop.utils.ConvertUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;

/**
 * @author LikeGirl
 * @version v1.0
 * @title: BaseService
 * @description: TODO
 * @date 2019/1/18 16:15
 */
public abstract class BaseService<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseService.class);

    @Autowired
    protected HbaseTemplate hbaseTemplate;

    @Autowired
    protected Connection connection;

    public List<T> select1(String table, String family, String startRow, String endRow, String[] cols) {
        LOGGER.error("开始查询： time [{}]",System.currentTimeMillis());
        Scan scan = new Scan();
        scan.setStartRow(startRow.getBytes());
        scan.setStopRow(endRow.getBytes());
        setScanProperties(scan, family, cols);
        return hbaseTemplate.find(table, scan, new ResultsExtractor<List<T>>() {
            @Override
            public List<T> extractData(ResultScanner results) throws Exception {
                LOGGER.error("开始解析： time [{}]",System.currentTimeMillis());
                List<T> result = new ArrayList<>();
                for(Result r : results){
                    result.add(newInstanceT(r, family));
                }
                LOGGER.error("完成解析： time [{}]",System.currentTimeMillis());
                return result;
            }
        });
    }

    public List<T> select(String table, String family, String startRow, String endRow, String[] cols) {
        Scan scan = new Scan();
        scan.setStartRow(startRow.getBytes());
        scan.setStopRow(endRow.getBytes());
        setScanProperties(scan, family, cols);
        return hbaseTemplate.find(table, scan, new RowMapper<T>() {
            @Override
            public T mapRow(Result result, int i) throws Exception {
                return newInstanceT(result, family);
            }
        });
    }

    /**
     * 使用RowKey查询一行数据
     *
     * @param table  表名称
     * @param family 列族
     * @param rowKey row-key
     * @param cols   查询的列
     *               默认为NULL,查询所有列
     * @return T
     */
    public T selectOne(String table, String family, String rowKey, String[] cols) {
        return hbaseTemplate.execute(table, new TableCallback<T>() {
            @Override
            public T doInTable(HTableInterface hTableInterface) throws Throwable {
                Get get = new Get(rowKey.getBytes());
                setGetProperties(get, family, cols);
                Result result = hTableInterface.get(get);
                return newInstanceT(result, family);
            }
        });
    }

    /**
     * @param table      表名称
     * @param family     列族
     * @param rowKeyLink row-key prefix
     *                   示例： profix_timestamp => vin_timestamp
     * @return List<T>
     */
    public List<T> select(String table, String family, String rowKeyLink, String[] cols) {
        return hbaseTemplate.execute(table, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(HTableInterface hTableInterface) throws Throwable {
                List<T> data = new ArrayList<>();
                FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                PrefixFilter prefixFilter = new PrefixFilter(rowKeyLink.getBytes());
                filterList.addFilter(prefixFilter);
                Scan scan = new Scan();
                setScanProperties(scan, family, cols);
                scan.setFilter(filterList);
                ResultScanner scanner = hTableInterface.getScanner(scan);
                for (Result result : scanner) {
                    data.add(newInstanceT(result, family));
                }
                return data;
            }
        });
    }

    public void insert(String table, String family, T t) {
        hbaseTemplate.execute(table, new TableCallback<Object>() {
            @Override
            public Object doInTable(HTableInterface table) throws Throwable {
                table.put(objectToPut(family, t));
                return null;
            }
        });
    }

    public void insertList(String table, String family, List<T> ts) {
        hbaseTemplate.execute(table, new TableCallback<Object>() {
            @Override
            public Object doInTable(HTableInterface table) throws Throwable {
                List<Put> puts = new ArrayList<>();
                for (T t : ts) {
                    puts.add(objectToPut(family, t));
                }
                table.put(puts);
                return null;
            }
        });
    }


    @SuppressWarnings("unchecked")
    public Class<T> getTClass() {
        return (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    public Put objectToPut(String family, T t) throws IllegalAccessException {
        Field rowKeyField = getRowKeyField();
        Object rowKey = rowKeyField.get(t);
        Put put = new Put(ConvertUtil.convert(rowKey).getBytes());
        Field[] fields = getTClass().getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            String key = field.getName();
            Object value = field.get(t);
            if (key != null && value != null) {
                String val = ConvertUtil.convert(value);
                put.addColumn(family.getBytes(), key.getBytes(), val.getBytes());
            }
        }
        return put;
    }

    public T newInstanceT(Result result, String family) throws IllegalAccessException, InstantiationException {
        Field[] fields = getTClass().getDeclaredFields();
        T t = getTClass().newInstance();
        Field rf = getRowKeyField();
        if (rf != null) {
            rf.setAccessible(true);
            rf.set(t, Bytes.toString(result.getRow()));
        }
        for (Field field : fields) {
            field.setAccessible(true);
            String name = field.getName();
            String value = Bytes.toString(result.getValue(family.getBytes(), name.getBytes()));
            if (value != null) {
                Class<?> fileType = field.getType();
                ConvertUtil.registerDateConvert();
                field.set(t, ConvertUtil.convert(value, fileType));
            }
        }
        return t;
    }

    public String getRowKey() {
        if (getTClass().isAnnotationPresent(RowKey.class)) {
            RowKey rowKey = getTClass().getAnnotation(RowKey.class);
            return rowKey.value();
        }
        return null;
    }

    public Field getRowKeyField() {
        String rowKey = getRowKey();
        if (StringUtils.isNotBlank(rowKey)) {
            try {
                return getTClass().getDeclaredField(rowKey);
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public void setScanProperties(Scan scan, String family, String... cols) {
        if (cols != null && cols.length > 0) {
            for (String col : cols) {
                scan.addColumn(family.getBytes(), col.getBytes());
            }
        } else {
            LOGGER.warn("Scan set addColumn: cols is empty");
        }
    }

    public void setGetProperties(Get get, String family, String... cols) {
        if (cols != null && cols.length > 0) {
            for (String col : cols) {
                get.addColumn(family.getBytes(), col.getBytes());
            }
        } else {
            LOGGER.warn("Get set addColumn: cols is empty");
        }
    }

}
