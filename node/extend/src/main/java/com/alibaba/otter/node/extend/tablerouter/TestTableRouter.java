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
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author weishuichao
 * @version $Id: TestTableRouter.java,v 0.1 2019年10月03日 11:29 $Exp
 */
public class TestTableRouter extends AbstractTableRouter {
    @Override
    public String dbRoute(EventData rowData, DataMedia.ModeValue sourceSchemas, DataMedia.ModeValue targetSchemas) {
       return targetSchemas.getSingleValue();
    }

    @Override
    public String tableRoute(EventData rowData, DataMedia.ModeValue sourceTables, DataMedia.ModeValue targetTables) {
        List<EventColumn> eventColumns = rowData.getColumns();
        String columnValue="";
        for(EventColumn column : eventColumns){
            if(column.getColumnName().equalsIgnoreCase("createTime")){
                columnValue= column.getColumnValue();
                break;
            }
        }
        Date date =new SimpleDateFormat("yyyy-MM-dd").parse(columnValue,new ParsePosition(0));
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return "product_"+calendar.get(Calendar.YEAR);
    }
}