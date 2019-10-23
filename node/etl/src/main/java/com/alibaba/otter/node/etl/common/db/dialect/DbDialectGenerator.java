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

import com.alibaba.otter.node.etl.common.db.dialect.elastic.ElasticSearchDialect;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.lob.LobHandler;

import com.alibaba.otter.node.etl.common.db.dialect.mysql.MysqlDialect;
import com.alibaba.otter.node.etl.common.db.dialect.oracle.OracleDialect;
import com.alibaba.otter.shared.common.model.config.data.DataMediaType;

/**
 * @author zebin.xuzb @ 2012-8-8
 * @version 4.1.0
 */
public class DbDialectGenerator {

    protected static final String ORACLE      = "oracle";
    protected static final String MYSQL       = "mysql";
    protected static final String TDDL_GROUP  = "TGroupDatabase";
    protected static final String TDDL_CLIENT = "TDDL";

    protected LobHandler          defaultLobHandler;
    protected LobHandler          oracleLobHandler;

    protected DbDialect generate(Object dbconn, String databaseName, String databaseNameVersion,
                                 int databaseMajorVersion, int databaseMinorVersion, DataMediaType dataMediaType) {
        DbDialect dialect = null;
        if (dataMediaType.isElasticSearch()) {
            dialect = new ElasticSearchDialect((RestHighLevelClient) dbconn, databaseName, databaseMajorVersion,
                    databaseMinorVersion);
        }else  if(dataMediaType.isOracle()){
            dialect = new OracleDialect((JdbcTemplate)dbconn,
                    oracleLobHandler,
                    databaseName,
                    databaseMajorVersion,
                    databaseMinorVersion);
        }else if(dataMediaType.isMysql()){
            JdbcTemplate jdbcTemplate = (JdbcTemplate) dbconn;
            dialect = new MysqlDialect(jdbcTemplate,
                    defaultLobHandler,
                    databaseName,
                    databaseNameVersion,
                    databaseMajorVersion,
                    databaseMinorVersion);
        }else{
            throw new RuntimeException(databaseName + " type is not support!");
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
