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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kafka.clients.producer.Producer;
import org.elasticsearch.client.Client;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.lob.LobHandler;

import com.alibaba.otter.node.etl.common.db.dialect.arvo.HdfsArvoDialect;
import com.alibaba.otter.node.etl.common.db.dialect.cassandra.CassandraDialect;
import com.alibaba.otter.node.etl.common.db.dialect.elasticsearch.ElasticSearchDialect;
import com.alibaba.otter.node.etl.common.db.dialect.greenplum.GreenPlumDialect;
import com.alibaba.otter.node.etl.common.db.dialect.hbase.HBaseDialect;
import com.alibaba.otter.node.etl.common.db.dialect.kafka.KafkaDialect;
import com.alibaba.otter.node.etl.common.db.dialect.mysql.MysqlDialect;
import com.alibaba.otter.node.etl.common.db.dialect.oracle.OracleDialect;
import com.alibaba.otter.shared.common.model.config.data.DataMediaType;
import com.datastax.driver.core.Cluster;

/**
 * @author zebin.xuzb @ 2012-8-8
 * @version 4.1.0
 */
public class DbDialectGenerator {

	protected static final String ORACLE = "oracle";
	protected static final String MYSQL = "mysql";
	protected static final String GREENPLUM = "greenplum";
	protected static final String TDDL_GROUP = "TGroupDatabase";
	protected static final String TDDL_CLIENT = "TDDL";

	protected LobHandler defaultLobHandler;
	protected LobHandler oracleLobHandler;

	protected DbDialect generate(Object dbconn, String databaseName, int databaseMajorVersion, int databaseMinorVersion,
			DataMediaType dataMediaType) {
		DbDialect dialect = null;
		if (dataMediaType.isElasticSearch()) {
			dialect = new ElasticSearchDialect((Client) dbconn, databaseName, databaseMajorVersion,
					databaseMinorVersion);
		} else if (dataMediaType.isCassandra()) {
			dialect = new CassandraDialect((Cluster) dbconn, databaseName, databaseMajorVersion,
					databaseMinorVersion);
		} else if (dataMediaType.isHBase()) {
			dialect = new HBaseDialect((Connection) dbconn, databaseName, databaseMajorVersion,
					databaseMinorVersion);
		} else if (dataMediaType.isHDFSArvo()) {
			dialect = new HdfsArvoDialect((FileSystem) dbconn, databaseName, databaseMajorVersion,
					databaseMinorVersion);
		} else if (dataMediaType.isKafka()) {
			dialect = new KafkaDialect((Producer) dbconn, databaseName, databaseMajorVersion,
					databaseMinorVersion);
		} else if (dataMediaType.isGreenPlum()) {
			dialect = new GreenPlumDialect((JdbcTemplate) dbconn, defaultLobHandler,databaseName, databaseMajorVersion,
					databaseMinorVersion);
		} else {
			JdbcTemplate jdbcTemplate = (JdbcTemplate) dbconn;
			if (StringUtils.startsWithIgnoreCase(databaseName, ORACLE)) { // for
				dialect = new OracleDialect(jdbcTemplate, oracleLobHandler, databaseName, databaseMajorVersion,
						databaseMinorVersion);
			} else if (StringUtils.startsWithIgnoreCase(databaseName, MYSQL)) { // for
																				// mysql
				dialect = new MysqlDialect(jdbcTemplate, defaultLobHandler, databaseName, databaseMajorVersion,
						databaseMinorVersion);
			}  else if (StringUtils.startsWithIgnoreCase(databaseName, TDDL_GROUP)) { // for
																						// tddl
																						// group
				throw new RuntimeException(databaseName + " type is not support!");
			} else if (StringUtils.startsWithIgnoreCase(databaseName, TDDL_CLIENT)) {
				throw new RuntimeException(databaseName + " type is not support!");
			}
		}
		return dialect;
	}

	// ======== setter =========
	public void setDefaultLobHandler(LobHandler defaultLobHandler) {
		this.defaultLobHandler = defaultLobHandler;
	}

	public void setOracleLobHandler(LobHandler oracleLobHandler) {
		this.oracleLobHandler = oracleLobHandler;
	}
}
