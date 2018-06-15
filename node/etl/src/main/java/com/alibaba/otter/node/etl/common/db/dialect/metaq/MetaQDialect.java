package com.alibaba.otter.node.etl.common.db.dialect.metaq;

import org.apache.ddlutils.model.Table;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

import com.alibaba.otter.node.etl.common.db.dialect.AbstraNoSQLDialect;
import com.alibaba.otter.node.etl.common.db.dialect.NoSqlTemplate;

public class MetaQDialect extends AbstraNoSQLDialect {

	private DefaultMQProducer producer=null;
	private NoSqlTemplate nosqlTemplate;
	
	public MetaQDialect(DefaultMQProducer producer,String databaseName, int databaseMajorVersion,
			int databaseMinorVersion){
		this.producer = producer;
		this.databaseName = databaseName;
		this.databaseMajorVersion = databaseMajorVersion;
		this.databaseMinorVersion = databaseMinorVersion;
		nosqlTemplate=new MetaQTemplate(producer); 
	}
	@Override
	public boolean isEmptyStringNulled() {
		return false;
	}

	@Override
	public DefaultMQProducer getJdbcTemplate() {
		return producer;
	}

	@Override
	public NoSqlTemplate getSqlTemplate() {
		return nosqlTemplate;
	}

	@Override
	public Table findTable(String schema, String table) {
		return null;
	}

	@Override
	public Table findTable(String schema, String table, boolean useCache) {
		return null;
	}

	@Override
	public void reloadTable(String schema, String table) {
		
	}

	@Override
	public void destory() {
	}

}
