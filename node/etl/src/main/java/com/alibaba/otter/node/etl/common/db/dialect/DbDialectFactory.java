/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.otter.node.etl.common.db.dialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.sql.DataSource;

import com.alibaba.dubbo.rpc.cluster.Cluster;
import com.google.common.cache.*;
import com.oracle.jrockit.jfr.Producer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import com.alibaba.otter.node.etl.common.datasource.DataSourceService;
import com.alibaba.otter.shared.common.model.config.data.db.DbMediaSource;
import com.google.common.base.Function;
import com.google.common.collect.MigrateMap;
import com.google.common.collect.OtterMigrateMap;
import com.google.common.collect.OtterMigrateMap.OtterRemovalListener;

/**
 * @author jianghang 2011-10-27 下午02:12:06
 * @version 4.0.0
 */
public class DbDialectFactory implements DisposableBean {

    private static final Logger logger = LoggerFactory.getLogger(DbDialectFactory.class);
    private DataSourceService dataSourceService;
    private DbDialectGenerator dbDialectGenerator;

    // 第一层pipelineId , 第二层DbMediaSource id
    private LoadingCache<Long, LoadingCache<DbMediaSource, DbDialect>> dialects;

    public DbDialectFactory() {

        // 构建第一层map
        CacheBuilder<Long, LoadingCache<DbMediaSource, DbDialect>> cacheBuilder = CacheBuilder.newBuilder().maximumSize(1000)
                .softValues().removalListener(new RemovalListener<Long, LoadingCache<DbMediaSource, DbDialect>>() {

                    @Override
                    public void onRemoval(
                            RemovalNotification<Long, LoadingCache<DbMediaSource, DbDialect>> paramRemovalNotification) {
                        if (paramRemovalNotification.getValue() == null) {
                            return;
                        }

                        for (DbDialect dbDialect : paramRemovalNotification.getValue().asMap().values()) {
                            dbDialect.destory();
                        }
                    }
                });

        dialects = cacheBuilder.build(new CacheLoader<Long, LoadingCache<DbMediaSource, DbDialect>>() {
            @Override
            public LoadingCache<DbMediaSource, DbDialect> load(final Long pipelineId) throws Exception {
                return CacheBuilder.newBuilder().maximumSize(1000)
                        .build(new CacheLoader<DbMediaSource, DbDialect>() {

                            @SuppressWarnings("unchecked")
                            @Override
                            public DbDialect load(final DbMediaSource source) throws Exception {
                                Object dbconn = dataSourceService.getDataSource(pipelineId, source);
                                if (source.getType().isElasticSearch()) {
                                    Client client = (Client) dbconn;
                                    DbDialect dialect = dbDialectGenerator.generate(client, "ElasticSearch", "", 2,
                                            3, source.getType());
                                    if (dialect == null) {
                                        throw new UnsupportedOperationException("no dialect for" + "ElasticSearch");
                                    }
                                    if (logger.isInfoEnabled()) {
                                        logger.info(String.format("--- DATABASE: %s, SCHEMA: %s ---",
                                                "ElasticSearch",
                                                (dialect.getDefaultSchema() == null)
                                                        ? dialect.getDefaultCatalog()
                                                        : dialect.getDefaultSchema()));
                                    }
                                    return dialect;
                                } else if (source.getType().isMysql() || source.getType().isOracle()) {
                                    DataSource dataSource = (DataSource) dbconn;
                                    final JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
                                    return (DbDialect) jdbcTemplate.execute(new ConnectionCallback() {
                                        @Override
                                        public Object doInConnection(Connection c)
                                                throws SQLException, DataAccessException {
                                            DatabaseMetaData meta = c.getMetaData();
                                            String databaseName = meta.getDatabaseProductName();
                                            int databaseMajorVersion = meta.getDatabaseMajorVersion();
                                            int databaseMinorVersion = meta.getDatabaseMinorVersion();
                                            DbDialect dialect = dbDialectGenerator.generate(jdbcTemplate,
                                                    databaseName, meta.getDatabaseProductVersion(), databaseMajorVersion, databaseMinorVersion,
                                                    source.getType());
                                            if (dialect == null) {
                                                throw new UnsupportedOperationException(
                                                        "no dialect for" + databaseName);
                                            }
                                            if (logger.isInfoEnabled()) {
                                                logger.info(String.format("--- DATABASE: %s, SCHEMA: %s ---",
                                                        databaseName,
                                                        (dialect.getDefaultSchema() == null)
                                                                ? dialect.getDefaultCatalog()
                                                                : dialect.getDefaultSchema()));
                                            }
                                            return dialect;
                                        }
                                    });
                                }else{
                                    throw new UnsupportedOperationException("no dialect for" + source.getType());
                                }
                            }
                        });
            }
        });


//        dialects = OtterMigrateMap.makeSoftValueComputingMapWithRemoveListenr(new Function<Long, Map<DbMediaSource, DbDialect>>() {
//
//            public Map<DbMediaSource, DbDialect> apply(final Long pipelineId) {
//                // 构建第二层map
//                return MigrateMap.makeComputingMap(new Function<DbMediaSource, DbDialect>() {
//
//                    public DbDialect apply(final DbMediaSource source) {
//                        DataSource dataSource = dataSourceService.getDataSource(pipelineId, source);
//                        final JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
//                        return (DbDialect) jdbcTemplate.execute(new ConnectionCallback() {
//
//                            public Object doInConnection(Connection c) throws SQLException, DataAccessException {
//                                DatabaseMetaData meta = c.getMetaData();
//                                String databaseName = meta.getDatabaseProductName();
//                                String databaseVersion = meta.getDatabaseProductVersion();
//                                int databaseMajorVersion = meta.getDatabaseMajorVersion();
//                                int databaseMinorVersion = meta.getDatabaseMinorVersion();
//                                DbDialect dialect = dbDialectGenerator.generate(jdbcTemplate,
//                                    databaseName,
//                                    databaseVersion,
//                                    databaseMajorVersion,
//                                    databaseMinorVersion,
//                                    source.getType());
//                                if (dialect == null) {
//                                    throw new UnsupportedOperationException("no dialect for" + databaseName);
//                                }
//
//                                if (logger.isInfoEnabled()) {
//                                    logger.info(String.format("--- DATABASE: %s, SCHEMA: %s ---",
//                                        databaseName,
//                                        (dialect.getDefaultSchema() == null) ? dialect.getDefaultCatalog() : dialect.getDefaultSchema()));
//                                }
//
//                                return dialect;
//                            }
//                        });
//
//                    }
//                });
//            }
//        },
//            new OtterRemovalListener<Long, Map<DbMediaSource, DbDialect>>() {
//
//                @Override
//                public void onRemoval(Long pipelineId, Map<DbMediaSource, DbDialect> dialect) {
//                    if (dialect == null) {
//                        return;
//                    }
//
//                    for (DbDialect dbDialect : dialect.values()) {
//                        dbDialect.destory();
//                    }
//                }
//
//            });

    }

    public DbDialect getDbDialect(Long pipelineId, DbMediaSource source) {
        try {
            return dialects.get(pipelineId).get(source);
        } catch (ExecutionException e) {
            logger.error("DbDialectFactory.getDbDialect failure,pipelineId:" + pipelineId + ";source:" + source.getType(), e);
            return null;
        }

        //return dialects.get(pipelineId).get(source);
    }

    public void destory(Long pipelineId) {
        try {
            LoadingCache<DbMediaSource, DbDialect> dialect = dialects.get(pipelineId);
            if (dialect != null) {
                for (DbDialect dbDialect : dialect.asMap().values()) {
                    dbDialect.destory();
                }
            }
            dialects.invalidate(pipelineId);
        } catch (ExecutionException e) {
            logger.error("DbDialectFactory.destory failure", e);
        }


//        Map<DbMediaSource, DbDialect> dialect = dialects.remove(pipelineId);
//        if (dialect != null) {
//            for (DbDialect dbDialect : dialect.values()) {
//                dbDialect.destory();
//            }
//        }
    }

    @Override
    public void destroy() throws Exception {
        Set<Long> pipelineIds = new HashSet<Long>(dialects.asMap().keySet());
        for (Long pipelineId : pipelineIds) {
            destory(pipelineId);
        }


//        Set<Long> pipelineIds = new HashSet<Long>(dialects.keySet());
//        for (Long pipelineId : pipelineIds) {
//            destory(pipelineId);
//        }
    }

    public void removeDbDialect(Long pipelineId, DbMediaSource source) {
        try {
            dialects.get(pipelineId).get(source).destory();
        } catch (ExecutionException e) {
           logger.error("DbDialectFactory.removeDbDialect",e);
        }
        try {
            LoadingCache<DbMediaSource, DbDialect> dialect = dialects.get(pipelineId);
            dialect.invalidate(source);
            dataSourceService.destroy(pipelineId, source);
        } catch (ExecutionException e) {
            logger.error("DbDialectFactory.removeDbDialect",e);
        }
    }


    // =============== setter / getter =================

    public void setDataSourceService(DataSourceService dataSourceService) {
        this.dataSourceService = dataSourceService;
    }

    public void setDbDialectGenerator(DbDialectGenerator dbDialectGenerator) {
        this.dbDialectGenerator = dbDialectGenerator;
    }

}
