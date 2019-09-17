package com.alibaba.otter.node.etl.common.db.dialect.elasticsearch;

import com.alibaba.otter.node.etl.common.db.dialect.AbstraNoSQLDialect;
import com.alibaba.otter.node.etl.common.db.dialect.NoSqlTemplate;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.cache.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.NestableRuntimeException;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class ElasticSearchDialect extends AbstraNoSQLDialect {
    private Client client = null;
    protected LoadingCache<List<String>, Table> tables;
    private NoSqlTemplate nosqlTemplate;

    private static Map<String,String> esTypeConvertMap=new HashMap<String, String>();
    static{
        esTypeConvertMap.put("text","VARCHAR");
        esTypeConvertMap.put("long","BIGINT");
        esTypeConvertMap.put("integer","INTEGER");
        esTypeConvertMap.put("short","SMALLINT");
        esTypeConvertMap.put("byte","SMALLINT");
        esTypeConvertMap.put("double","DOUBLE");
        esTypeConvertMap.put("float","FLOAT");
        esTypeConvertMap.put("date","TIMESTAMP");
        esTypeConvertMap.put("boolean","BIT");
        esTypeConvertMap.put("binary","BLOB");
    }

    public ElasticSearchDialect(Client dbconn, String databaseName, int databaseMajorVersion,
                                int databaseMinorVersion) {
        this.client = dbconn;
        this.databaseName = databaseName;
        this.databaseMajorVersion = databaseMajorVersion;
        this.databaseMinorVersion = databaseMinorVersion;
        initTables(client);
        this.nosqlTemplate = new ElasticSearchTemplate(client);
    }

    private void initTables(final Client client) {
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

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Table readTable(String schemaName, String tableName) {
        GetMappingsResponse mappingResp = client.admin().indices().prepareGetMappings(schemaName).setTypes(tableName).execute().actionGet();
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






    @SuppressWarnings("rawtypes")
    private String getFieldValue(String key, Map.Entry<String, Map> ob) {
        Object obj = ob.getValue().get(key);
        if (obj != null) {
            return (String) obj;
        }
        return null;
    }

    /**
     * es的空字符串设置未无字段数据写入
     */
    @Override
    public boolean isEmptyStringNulled() {
        return true;
    }

    @Override
    public boolean isDRDS() {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Client getJdbcTemplate() {
        return this.client;
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
    public Table findTable(String schema, String table) {
        return findTable(schema, table, true);
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
    }


    @SuppressWarnings("unchecked")
    @Override
    public NoSqlTemplate getSqlTemplate() {
        return nosqlTemplate;
    }

}
