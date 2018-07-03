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

package com.alibaba.otter.shared.common.model.config.data;

/**
 * @author jianghang 2011-9-2 上午11:36:21
 */
public enum DataMediaType {
	ROCKETMQ,
	HIVE,
	PRESTODB,
	SHARDINGJDBC,
	GREENPLUM,
    /** mysql DB */
    MYSQL,
    /** oracle DB */
    ORACLE,
    /** elsaticsearch  */
    ELASTICSEARCH,
    /** KAFKA DB */
    KAFKA,
    /** HBASE DB */
    HBASE,
    /** CASSANDRA DB */
    CASSANDRA,
    /** HDFS-ARVO DB */
    HDFS,
    /** cobar */
    COBAR,
    /** tddl */
    TDDL,
    /** cache */
    MEMCACHE,
    /** mq */
    MQ,
    /** napoli */
    NAPOLI,
    /** diamond push for us */
    DIAMOND_PUSH;
	public boolean isRocketMq() {
        return this.equals(DataMediaType.ROCKETMQ);
    }
	public boolean isHive() {
        return this.equals(DataMediaType.HIVE);
    }
	public boolean isPrestoDB() {
        return this.equals(DataMediaType.PRESTODB);
    }
	public boolean isShardingJdbc() {
        return this.equals(DataMediaType.SHARDINGJDBC);
    }

    public boolean isKafka() {
        return this.equals(DataMediaType.KAFKA);
    }

    public boolean isElasticSearch() {
        return this.equals(DataMediaType.ELASTICSEARCH);
    }
    
    public boolean isHBase() {
        return this.equals(DataMediaType.HBASE);
    }

    public boolean isCassandra() {
        return this.equals(DataMediaType.CASSANDRA);
    }
    public boolean isHDFS() {
        return this.equals(DataMediaType.HDFS);
    }

    public boolean isOracle() {
        return this.equals(DataMediaType.ORACLE);
    }
    public boolean isMysql() {
        return this.equals(DataMediaType.MYSQL);
    }


    public boolean isTddl() {
        return this.equals(DataMediaType.TDDL);
    }

    public boolean isCobar() {
        return this.equals(DataMediaType.COBAR);
    }

    public boolean isMemcache() {
        return this.equals(DataMediaType.MEMCACHE);
    }

    public boolean isMq() {
        return this.equals(DataMediaType.MQ);
    }
    
    public boolean isGreenPlum() {
        return this.equals(DataMediaType.GREENPLUM);
    }

    public boolean isNapoli() {
        return this.equals(DataMediaType.NAPOLI);
    }

    public boolean isDiamondPush() {
        return this.equals(DataMediaType.DIAMOND_PUSH);
    }
}
