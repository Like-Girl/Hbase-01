package cn.likegirl.hadoop.service;

import cn.likegirl.hadoop.model.TspCompleteCondition;

import java.util.Date;
import java.util.List;

/**
 * @author LikeGirl
 * @version v1.0
 * @title: CompleteConditionService
 * @description: TODO
 * @date 2019/1/18 16:16
 */
public interface CompleteConditionService {

    List<TspCompleteCondition> find(String vin, Date startDate, Date endDate);

    List<TspCompleteCondition> select(String vin, Date startDate, Date endDate);
}
