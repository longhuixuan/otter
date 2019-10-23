/*
 * frxs Inc.  湖南兴盛优选电子商务有限公司.
 * Copyright (c) 2017-2019. All Rights Reserved.
 */
package com.alibaba.otter.node.etl.common.db.dialect.elastic;

import com.alibaba.otter.node.etl.common.db.dialect.AbstractNoSQLDialect;
import com.alibaba.otter.node.etl.common.db.dialect.NoSqlTemplate;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.NestableRuntimeException;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author weishuichao
 * @version $Id: ElasticSearchDialect.java,v 0.1 2019年10月12日 10:33 $Exp
 */
public class ElasticSearchDialect extends AbstractNoSQLDialect {

    protected static final Logger logger = LoggerFactory.getLogger(ElasticSearchDialect.class);
    private final ElasticSearchTemplate elasticSearchTemplate;
    private final RestHighLevelClient client;

    private static Map<String, String> esTypeConvertMap = new HashMap<String, String>();

    static {
        esTypeConvertMap.put("text", "VARCHAR");
        esTypeConvertMap.put("long", "BIGINT");
        esTypeConvertMap.put("integer", "INTEGER");
        esTypeConvertMap.put("short", "SMALLINT");
        esTypeConvertMap.put("byte", "SMALLINT");
        esTypeConvertMap.put("double", "DOUBLE");
        esTypeConvertMap.put("float", "FLOAT");
        esTypeConvertMap.put("date", "TIMESTAMP");
        esTypeConvertMap.put("boolean", "BIT");
        esTypeConvertMap.put("binary", "BLOB");
    }


    public ElasticSearchDialect(RestHighLevelClient client, String databaseName, int databaseMajorVersion,
                                int databaseMinorVersion) {
        this.client = client;
        this.databaseName = databaseName;
        this.databaseMajorVersion = databaseMajorVersion;
        this.databaseMinorVersion = databaseMinorVersion;
        initTables(client);
        this.elasticSearchTemplate = new ElasticSearchTemplate(client);
    }


    private void initTables(final RestHighLevelClient client) {
        // soft引用设置，避免内存爆了
        this.tables = CacheBuilder.newBuilder().maximumSize(1000)
                .removalListener(new RemovalListener<List<String>, Table>() {
                    @Override
                    public void onRemoval(RemovalNotification<List<String>, Table> paramRemovalNotification) {
                        logger.warn("Eviction For Table:" + paramRemovalNotification.getValue());
                    }
                }).build(new CacheLoader<List<String>, Table>() {
                    @Override
                    public Table load(List<String> names) throws Exception {
                        Assert.isTrue(names.size() == 2);
                        try {
                            Table table = readTable(names.get(0), names.get(1));
                            if (table == null) {
                                throw new NestableRuntimeException(
                                        "no found table [" + names.get(0) + "." + names.get(1) + "] , pls check");
                            } else {
                                return table;
                            }
                        } catch (Exception e) {
                            throw new NestableRuntimeException(
                                    "find table [" + names.get(0) + "." + names.get(1) + "] error", e);
                        }
                    }
                });
    }

    // TODO  是不是有点问题？
    private Table readTable(String schemaName, String tableName) throws IOException {
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices(schemaName);
        request.types(tableName);
        GetMappingsResponse mappingResp = client.indices().getMapping(request, RequestOptions.DEFAULT);
        ImmutableOpenMap<String, MappingMetaData> mappings = mappingResp.getMappings().get(schemaName);
        if (mappings == null) {
            return null;
        }
        Table table = new Table();
        table.setName(tableName);
        table.setType("ElasticSearch");
        table.setCatalog(schemaName);
        table.setSchema(schemaName);
        table.setDescription(schemaName);
        Column pkColumn = new Column();
        pkColumn.setJavaName("id");
        pkColumn.setName("id");
        pkColumn.setPrimaryKey(true);
        pkColumn.setRequired(true);
        pkColumn.setType(esTypeConvertMap.get("text"));
        table.addColumn(pkColumn);
        for (ObjectObjectCursor<String, MappingMetaData> typeEntry : mappings) {
            if (tableName.equalsIgnoreCase(typeEntry.key)) {
                try {
                    Map<String, Object> fields = typeEntry.value.sourceAsMap();
                    Map mf = (Map) fields.get("properties");
                    Iterator iter = mf.entrySet().iterator();
                    while (iter.hasNext()) {
                        //字段
                        Map.Entry<String, Map> ob = (Map.Entry<String, Map>) iter.next();
                        Column column = new Column();
                        column.setName(ob.getKey());
                        column.setJavaName(ob.getKey());
                        column.setType(esTypeConvertMap.get(getFieldValue("type", ob)));
                        table.addColumn(column);
                    }
                } catch (Exception e) {
                    logger.error("ERROR ## ElasticSearch find table happen error!", e);
                }
            }
        }
        return table;
    }

    private String getFieldValue(String key, Map.Entry<String, Map> ob) {
        Object obj = ob.getValue().get(key);
        if (obj != null) {
            return (String) obj;
        }
        return null;
    }


    @Override
    public boolean isEmptyStringNulled() {
        return true;
    }

    @Override
    public boolean isDRDS() {
        return false;
    }

    @Override
    public RestHighLevelClient getJdbcTemplate() {
        return this.client;
    }

    @Override
    public ElasticSearchTemplate getSqlTemplate() {
        return this.elasticSearchTemplate;
    }

    @Override
    public Table findTable(String schema, String table) {
        return findTable(schema, table, true);
    }

    @Override
    public Table findTable(String schema, String table, boolean useCache) {
        List<String> key = Arrays.asList(schema, table);
        if (useCache == false) {
            tables.invalidate(key);
        }
        try {
            return tables.get(key);
        } catch (ExecutionException e) {
            return null;
        }
    }

    @Override
    public String getShardColumns(String schema, String table) {
        return null;
    }

    @Override
    public void reloadTable(String schema, String table) {
        if (StringUtils.isNotEmpty(table)) {
            tables.invalidateAll(Arrays.asList(schema, table));
        } else {
            // 如果没有存在表名，则直接清空所有的table，重新加载
            tables.invalidateAll();
            tables.cleanUp();
        }
    }

    @Override
    public void destory() {
        if (this.client != null) {
            try {
                this.client.close();
            } catch (IOException e) {
                logger.error("destory es failure", e);
            }
        }
    }
}