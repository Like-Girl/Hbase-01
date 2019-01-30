package cn.likegirl.hadoop.controller;


import cn.likegirl.hadoop.model.TspCompleteCondition;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author LikeGirl
 * @version v1.0
 * @title: CompleteConditionController
 * @description: TODO
 * @date 2019/1/18 16:48
 */
@RestController
@RequestMapping("/tc")
public class CompleteConditionController extends BaseController {

    @RequestMapping(value = "/{vin}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public Map<String,Object> find(@PathVariable("vin") String vin,
                                           @RequestParam("startDate") String startDate,
                                           @RequestParam("endDate") String endDate) {
        Map<String,Object> result = new HashMap<>();
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            List<TspCompleteCondition> data = completeConditionService.select(vin, format.parse(startDate), format.parse(endDate));
            result.put("total",data.size());
            result.put("data",data);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return result;
    }

}
