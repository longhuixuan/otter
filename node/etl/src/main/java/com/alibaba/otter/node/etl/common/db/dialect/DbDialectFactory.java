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
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.sql.DataSource;

import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.producer.Producer;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import com.alibaba.otter.node.etl.common.datasource.DataSourceService;
import com.alibaba.otter.shared.common.model.config.data.db.DbMediaSource;
import com.datastax.driver.core.Cluster;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

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
					public LoadingCache<DbMediaSource, DbDialect> load(Long pipelineId) throws Exception {
						return CacheBuilder.newBuilder().maximumSize(1000)
								.build(new CacheLoader<DbMediaSource, DbDialect>() {

									@SuppressWarnings("unchecked")
									@Override
									public DbDialect load(DbMediaSource source) throws Exception {
										Object dbconn = dataSourceService.getDataSource(pipelineId, source);
										if (source.getType().isElasticSearch()) {
											Client client = (Client) dbconn;
											DbDialect dialect = dbDialectGenerator.generate(client, "ElasticSearch", 2,
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
										} else if (source.getType().isHBase()) {
											org.apache.hadoop.hbase.client.Connection hbaseconn = (org.apache.hadoop.hbase.client.Connection) dbconn;
											DbDialect dialect = dbDialectGenerator.generate(hbaseconn, "HBase", 1, 0,
													source.getType());
											if (dialect == null) {
												throw new UnsupportedOperationException("no dialect for" + "HBase");
											}
											if (logger.isInfoEnabled()) {
												logger.info(String.format("--- DATABASE: %s, SCHEMA: %s ---", "HBase",
														(dialect.getDefaultSchema() == null)
																? dialect.getDefaultCatalog()
																: dialect.getDefaultSchema()));
											}
											return dialect;
										} else if (source.getType().isCassandra()) {
											Cluster cluster = (Cluster) dbconn;
											DbDialect dialect = dbDialectGenerator.generate(cluster, "Cassandra", 2, 0,
													source.getType());
											if (dialect == null) {
												throw new UnsupportedOperationException("no dialect for" + "Cassandra");
											}
											if (logger.isInfoEnabled()) {
												logger.info(
														String.format("--- DATABASE: %s, SCHEMA: %s ---", "Cassandra",
																(dialect.getDefaultSchema() == null)
																		? dialect.getDefaultCatalog()
																		: dialect.getDefaultSchema()));
											}
											return dialect;
										} else if (source.getType().isKafka()) {
											Producer kafkaprod = (Producer) dbconn;
											DbDialect dialect = dbDialectGenerator.generate(kafkaprod, "Kafka", 0, 9,
													source.getType());
											if (dialect == null) {
												throw new UnsupportedOperationException("no dialect for" + "Kafka");
											}
											if (logger.isInfoEnabled()) {
												logger.info(String.format("--- DATABASE: %s, SCHEMA: %s ---", "Kafka",
														(dialect.getDefaultSchema() == null)
																? dialect.getDefaultCatalog()
																: dialect.getDefaultSchema()));
											}
											return dialect;
										} else if (source.getType().isHDFSArvo()) {
											FileSystem fs = (FileSystem) dbconn;
											DbDialect dialect = dbDialectGenerator.generate(fs, "HDFS_Arvo", 2, 1,
													source.getType());
											if (dialect == null) {
												throw new UnsupportedOperationException("no dialect for" + "HDFS_Arvo");
											}
											if (logger.isInfoEnabled()) {
												logger.info(
														String.format("--- DATABASE: %s, SCHEMA: %s ---", "HDFS_Arvo",
																(dialect.getDefaultSchema() == null)
																		? dialect.getDefaultCatalog()
																		: dialect.getDefaultSchema()));
											}
											return dialect;
										} else {
											DataSource dataSource = (DataSource) dbconn;
											final JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
											return (DbDialect) jdbcTemplate.execute(new ConnectionCallback() {
												public Object doInConnection(Connection c)
														throws SQLException, DataAccessException {
													DatabaseMetaData meta = c.getMetaData();
													String databaseName = meta.getDatabaseProductName();
													int databaseMajorVersion = meta.getDatabaseMajorVersion();
													int databaseMinorVersion = meta.getDatabaseMinorVersion();
													DbDialect dialect = dbDialectGenerator.generate(jdbcTemplate,
															databaseName, databaseMajorVersion, databaseMinorVersion,
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
										}
									}
								});
					}
				});

	}

	public DbDialect getDbDialect(Long pipelineId, DbMediaSource source) {
		try {
			return dialects.get(pipelineId).get(source);
		} catch (ExecutionException e) {
			return null;
		}
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
			e.printStackTrace();
		}
	}

	public void destroy() throws Exception {
		Set<Long> pipelineIds = new HashSet<Long>(dialects.asMap().keySet());
		for (Long pipelineId : pipelineIds) {
			destory(pipelineId);
		}
	}
	
	public void removeDbDialect(Long pipelineId, DbMediaSource source){
		try {
			dialects.get(pipelineId).get(source).destory();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		try {
			LoadingCache<DbMediaSource, DbDialect> dialect = dialects.get(pipelineId);
			dialect.invalidate(source);
			dataSourceService.destroy(pipelineId, source);
		} catch (ExecutionException e) {
			e.printStackTrace();
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
