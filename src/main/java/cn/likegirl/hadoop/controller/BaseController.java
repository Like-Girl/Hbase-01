package cn.likegirl.hadoop.controller;

import cn.likegirl.hadoop.service.CompleteConditionService;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author LikeGirl
 * @version v1.0
 * @title: BaseController
 * @description: TODO
 * @date 2019/1/18 16:49
 */
public abstract class BaseController {

    @Autowired
    protected CompleteConditionService completeConditionService;
}
