package com.alibaba.otter.shared.etl.extend.tablerouter;

import com.alibaba.otter.shared.common.model.config.data.DataMedia.ModeValue;
import com.alibaba.otter.shared.etl.model.EventData;

/**
 * 〈分表路由〉<p>
 * 支持多对多映射
 * source_tab_[00-16] => target_tab_[2019-2029]
 *
 * @author zixiao
 * @date 2019/8/13
 */
public interface TableRouter {

    /**
     * 路由到目标db
     *
     * @param rowData
     * @param targetSchemas
     * @param targetSchemas
     * @return 目标库名
     */
    String dbRoute(EventData rowData, ModeValue sourceSchemas, ModeValue targetSchemas);

    /**
     * 路由到目标table
     *
     * @param rowData
     * @param sourceTables
     * @param targetTables
     * @return 目标表名
     */
    String tableRoute(EventData rowData, ModeValue sourceTables, ModeValue targetTables);

}
