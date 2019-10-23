/*
 * frxs Inc.  湖南兴盛优选电子商务有限公司.
 * Copyright (c) 2017-2019. All Rights Reserved.
 */
package com.alibaba.otter.node.etl.common.db.dialect.elastic;

import com.alibaba.otter.node.etl.common.db.dialect.NoSqlTemplate;
import com.alibaba.otter.node.etl.common.db.exception.ConnClosedException;
import com.alibaba.otter.node.etl.common.db.utils.SqlUtils;
import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author weishuichao
 * @version $Id: ElasticSearchTemplate.java,v 0.1 2019年10月07日 10:20 $Exp
 */
public class ElasticSearchTemplate implements NoSqlTemplate {

    private Logger logger = LoggerFactory.getLogger(ElasticSearchTemplate.class);

    private final RestHighLevelClient client;

    public ElasticSearchTemplate(RestHighLevelClient client) {
        this.client = client;
    }


    @Override
    public List<EventData> batchEventDatas(List<EventData> events) throws ConnClosedException, IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (EventData event : events) {
            String primaryKey = SqlUtils.getPrimaryKey(event);
            event.setExeResult(primaryKey);
            if (event.getEventType().isDelete()) {
                bulkRequest.add(new DeleteRequest(event.getSchemaName(), event.getTableName(), primaryKey));
            } else if (event.getEventType().isUpdate()) {
                bulkRequest.add(new IndexRequest(event.getSchemaName(), event.getTableName(), primaryKey).source(convertMap(event)));
            } else if (event.getEventType().isInsert()) {
                bulkRequest.add(new IndexRequest(event.getSchemaName(), event.getTableName(), primaryKey).source(convertMap(event)));
            }
        }
        BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        for (BulkItemResponse item : response.getItems()) {
            if (item.isFailed()) {
                EventData event = getEventData(events, item.getId());
                if (event != null) {
                    event.setExeResult("失败：id [" + item.getId() + "], message [" + item.getFailureMessage() + "]");
                }
                logger.error("失败：id [" + item.getId() + "], message [" + item.getFailureMessage() + "]");
            }
        }
        return events;
    }


    //TODO   可能存在问题
    private Map convertMap(EventData event) {
        Map recordM = new HashMap();
        for (EventColumn column : event.getKeys()) {
            if (StringUtils.isNotEmpty(column.getColumnValue())) {
                recordM.put(column.getColumnName(), column.getColumnValue());
            }
        }
        for (EventColumn column : event.getColumns()) {
            if (StringUtils.isNotEmpty(column.getColumnValue())) {
                recordM.put(column.getColumnName(), column.getColumnValue());
            }
        }
        return recordM;
    }


    private EventData getEventData(List<EventData> events, String pk) {
        for (EventData event : events) {
            if (pk.equalsIgnoreCase(event.getExeResult())) {
                return event;
            }
        }
        return null;
    }


    @Override
    public void distory() throws ConnClosedException {
        try {
            client.close();
        } catch (IOException e) {
            logger.error("关闭ElasticSearch failure", e);
        }
    }

    @Override
    public EventData insertEventData(EventData event) throws ConnClosedException, IOException {
        String pkVal = SqlUtils.getPrimaryKey(event);
        event.setExeResult(pkVal);
        if (event.getEventType().isInsert()) {
            IndexRequest indexRequest = new IndexRequest(event.getSchemaName(), event.getTableName(), pkVal).source(convertMap(event));
            IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
            if (response.getResult() != DocWriteResponse.Result.CREATED) {
                logger.error("记录ID：" + pkVal + "  建立失败！");
                event.setExeResult("失败:记录ID：" + pkVal + "  建立失败！");
                return event;
            }
        }
        return null;
    }

    @Override
    public EventData updateEventData(EventData event) throws ConnClosedException, IOException {
        String pkVal = SqlUtils.getPrimaryKey(event);
        if (event.getEventType().isUpdate()) {
            UpdateRequest updateRequest = new UpdateRequest(event.getSchemaName(), event.getTableName(), pkVal).doc(convertMap(event));
            UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
            if (updateResponse.getResult() != DocWriteResponse.Result.UPDATED) {
                logger.error("记录ID：" + pkVal + "  更新失败！");
                event.setExeResult("失败: 记录ID：" + pkVal + "  更新失败！");
                return event;
            }
        }
        return null;
    }

    @Override
    public EventData deleteEventData(EventData event) throws ConnClosedException, IOException {
        String pkVal = SqlUtils.getPrimaryKey(event);
        // 删除
        if (event.getEventType().isDelete()) {
            DeleteRequest deleteRequest = new DeleteRequest(event.getSchemaName(), event.getTableName(), pkVal);
            DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
            if (response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                logger.error("记录ID：" + pkVal + "  删除失败！");
                event.setExeResult("失败:记录ID：" + pkVal + "  删除失败！");
                return event;
            }
        }
        return null;
    }

    @Override
    public EventData createTable(EventData event) throws ConnClosedException, IOException {
        logger.info("elasticsearch不支持创建表结构，目前可通过自动开启创建索引!");
        return event;

//
//        String pkVal = SqlUtils.getPrimaryKey(event);
//        GetRequest getRequest = new GetRequest(event.getDdlSchemaName());
//        getRequest.type(event.getTableName());
//        boolean exist = client.exists(getRequest,RequestOptions.DEFAULT);
//        if(!exist){
//            PutMappingRequest mapping = Requests.putMappingRequest(event.getDdlSchemaName())
//                    .type(event.getTableName());
//            PutMappingResponse response = client.indices().putMapping(mapping,RequestOptions.DEFAULT);
//            if (!response.isAcknowledged()) {
//                logger.error("建立表失败:" + response.toString());
//                event.setExeResult("失败:建立表:" + response.toString());
//                return event;
//            } else {
//                logger.debug("新建索引：" + event.getDdlSchemaName() + "下的type:" + event.getTableName() + " 成功！");
//            }
//        }
//        return event;
    }

    @Override
    public EventData alterTable(EventData event) throws ConnClosedException {
        logger.info("elasticsearch不支持修改表结构!");
        return event;
    }

    @Override
    public EventData deleteTable(EventData event) throws ConnClosedException {
        logger.info("elasticsearch不支持删除表结构!");
        return event;
    }

    @Override
    public EventData truncateTable(EventData event) throws ConnClosedException {
        logger.info("保护数据需求，elasticsearch不支持清空表数据!");
        return event;
    }

    @Override
    public EventData renameTable(EventData event) throws ConnClosedException {
        logger.info("保护数据需求，elasticsearch不删除原表!");
        return event;
    }
}