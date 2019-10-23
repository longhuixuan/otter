/*
 * frxs Inc.  湖南兴盛优选电子商务有限公司.
 * Copyright (c) 2017-2019. All Rights Reserved.
 */
package com.alibaba.otter.node.etl.common.db.dialect;

import com.alibaba.otter.node.etl.common.db.exception.ConnClosedException;
import com.alibaba.otter.shared.etl.model.EventData;

import java.io.IOException;
import java.util.List;

/**
 * @author weishuichao
 * @version $Id: NoSqlTemplate.java,v 0.1 2019年10月07日 10:17 $Exp
 */
public interface  NoSqlTemplate{
    /**
     * 批量执行dml数据操作，增，删，改
     *
     * @param events
     * @return 执行失败的记录集合返回,失败原因消息保存在exeResult字段中
     */
    public List<EventData> batchEventDatas(List<EventData> events) throws ConnClosedException, IOException;

    /**
     * 关闭连接
     * @throws ConnClosedException
     */
    public void distory() throws ConnClosedException;

    /**
     * 插入行数据
     *
     * @param event
     * @return 记录返回,失败原因消息保存在exeResult字段中
     */
    public EventData insertEventData(EventData event) throws ConnClosedException, IOException;

    /**
     * 更新行数句
     *
     * @param event
     * @return 记录返回,失败原因消息保存在exeResult字段中
     */
    public EventData updateEventData(EventData event) throws ConnClosedException, IOException;

    /**
     * 删除记录
     *
     * @param event
     * @return 记录返回,失败原因消息保存在exeResult字段中
     */
    public EventData deleteEventData(EventData event) throws ConnClosedException, IOException;

    /**
     * 建立表
     *
     * @param event
     * @return
     */
    public EventData createTable(EventData event) throws ConnClosedException, IOException;

    /**
     * 修改表
     *
     * @param event
     * @return
     */
    public EventData alterTable(EventData event) throws ConnClosedException;

    /**
     * 删除表
     *
     * @param event
     * @return
     */
    public EventData deleteTable(EventData event) throws ConnClosedException;

    /**
     * 清空表
     *
     * @param event
     * @return
     */
    public EventData truncateTable(EventData event) throws ConnClosedException;

    /**
     * 改名表
     *
     * @param event
     * @return
     */
    public EventData renameTable(EventData event) throws ConnClosedException;

}