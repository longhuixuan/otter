/*
 * frxs Inc.  湖南兴盛优选电子商务有限公司.
 * Copyright (c) 2017-2019. All Rights Reserved.
 */
package com.alibaba.otter.node.extend.tablerouter;

import com.alibaba.otter.shared.common.model.config.data.DataMedia;
import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author weishuichao
 * @version $Id: TestTableRouter.java,v 0.1 2019年10月03日 11:29 $Exp
 */
public class ProductTableRouter extends AbstractTableRouter {
    @Override
    public String dbRoute(EventData rowData, DataMedia.ModeValue sourceSchemas, DataMedia.ModeValue targetSchemas) {
        List<EventColumn> eventColumns = rowData.getColumns();
        Map<String,String> columMap= new HashMap<String,String>();
        for(EventColumn column : eventColumns){
            columMap.put(column.getColumnName(),column.getColumnValue());
        }
        Date createTime =new SimpleDateFormat("yyyy-MM-dd").parse(columMap.get("createTime"),new ParsePosition(0));
        String createTimeText=new SimpleDateFormat("yyyyMM").format(createTime);
        return "dev_trade_area_store_product_daily_"+columMap.get("areaId")+"_"+createTimeText;

    }

    @Override
    public String tableRoute(EventData rowData, DataMedia.ModeValue sourceTables, DataMedia.ModeValue targetTables) {
        return targetTables.getSingleValue();
    }
}