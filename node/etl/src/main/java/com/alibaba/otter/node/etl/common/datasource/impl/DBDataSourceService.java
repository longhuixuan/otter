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

package com.alibaba.otter.node.etl.common.datasource.impl;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.common.push.datasource.DataSourceHanlder;
import com.alibaba.otter.node.etl.common.datasource.DataSourceService;
import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;
import com.alibaba.otter.shared.common.model.config.data.DataMediaType;
import com.alibaba.otter.shared.common.model.config.data.db.DbMediaSource;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * Comment of DataSourceServiceImpl
 * 
 * @author xiaoqing.zhouxq
 * @author zebinxu, add {@link DataSourceHanlder}
 */
public class DBDataSourceService implements DataSourceService, DisposableBean {

	private static final Logger logger = LoggerFactory.getLogger(DBDataSourceService.class);

	private List<DataSourceHanlder> dataSourceHandlers;

	private int maxWait = 60 * 1000;

	private int minIdle = 0;

	private int initialSize = 0;

	private int maxActive = 16;

	private int maxIdle = 16;

	private int numTestsPerEvictionRun = -1;

	private int timeBetweenEvictionRunsMillis = 60 * 1000;

	private int removeAbandonedTimeout = 5 * 60;

	private int minEvictableIdleTimeMillis = 5 * 60 * 1000;

	/**
	 * 一个pipeline下面有一组DataSource.<br>
	 * key = pipelineId<br>
	 * value = key(dataMediaSourceId)-value(DataSource)<br>
	 */
	private LoadingCache<Long, LoadingCache<DbMediaSource, Object>> dataSources;

	public DBDataSourceService() {
		// 设置soft策略
		CacheBuilder<Long, LoadingCache<DbMediaSource, Object>> cacheBuilder = CacheBuilder.newBuilder().softValues()
				.removalListener(new RemovalListener<Long, LoadingCache<DbMediaSource, Object>>() {
					@Override
					public void onRemoval(RemovalNotification<Long, LoadingCache<DbMediaSource, Object>> paramR) {
						if (dataSources == null) {
							return;
						}
						for (Object dbconn : paramR.getValue().asMap().values()) {
							try {
								if (dbconn instanceof DataSource) {
									DataSource source = (DataSource) dbconn;
									// for filter to destroy custom datasource
									if (letHandlerDestroyIfSupport(paramR.getKey(), source)) {
										continue;
									} // fallback for regular destroy TODO need
										// to integrate to handler
									BasicDataSource basicDataSource = (BasicDataSource) source;
									basicDataSource.close();
								} else if (dbconn instanceof Client) {
									Client client = (Client) dbconn;
									client.close();
								} else if (dbconn instanceof Connection) {
									Connection hbaseconn = (Connection) dbconn;
									hbaseconn.close();
								} else if (dbconn instanceof Producer) {
									Producer producer = (Producer) dbconn;
									producer.close();
								} else if (dbconn instanceof Cluster) {
									Cluster producer = (Cluster) dbconn;
									producer.close();
								} else if (dbconn instanceof FileSystem) {
									FileSystem filesystem = (FileSystem) dbconn;
									filesystem.close();
								}
							} catch (SQLException | IOException e) {
								logger.error("ERROR ## close the datasource has an error", e);
							}
						}
					}
				});

		// 构建第一层map
		dataSources = cacheBuilder.build(new CacheLoader<Long, LoadingCache<DbMediaSource, Object>>() {
			@Override
			public LoadingCache<DbMediaSource, Object> load(Long pipelineId) throws Exception {
				return CacheBuilder.newBuilder().maximumSize(1000).build(new CacheLoader<DbMediaSource, Object>() {
					@Override
					public Object load(DbMediaSource dbMediaSource) throws Exception {
						// 扩展功能,可以自定义一些自己实现的 dataSource
						if (dbMediaSource.getType().isCassandra()) {
							return getCluster(dbMediaSource);
						} else if (dbMediaSource.getType().isElasticSearch()) {
							return getClient(dbMediaSource);
						} else if (dbMediaSource.getType().isHBase()) {
							return getHBaseConnection(dbMediaSource);
						} else if (dbMediaSource.getType().isHDFS()) {
							return getHDFS(dbMediaSource);
						} else if (dbMediaSource.getType().isKafka()) {
							return getProducer(dbMediaSource);
						} else if (dbMediaSource.getType().isRocketMq()) {
							return getMQProducer(dbMediaSource);
						} else {
							DataSource customDataSource = preCreate(pipelineId, dbMediaSource);
							if (customDataSource != null) {
								return customDataSource;
							}
							return createDataSource(dbMediaSource.getUrl(), dbMediaSource.getUsername(),
									dbMediaSource.getPassword(), dbMediaSource.getDriver(), dbMediaSource.getType(),
									dbMediaSource.getEncode());
						}
					}
				});
			}

		});

	}

	@SuppressWarnings("unchecked")
	public Object getDataSource(long pipelineId, DataMediaSource dataMediaSource) {
		Assert.notNull(dataMediaSource);
		DbMediaSource dbMediaSource = (DbMediaSource) dataMediaSource;
		try {
			return dataSources.get(pipelineId).get(dbMediaSource);
		} catch (ExecutionException e) {
			return null;
		}
	}

	/**
	 * 获取cassdran的连接
	 * 
	 * @param dataMediaSource
	 * @return
	 */
	public Cluster getCluster(DbMediaSource dbMediaSource) {
		Assert.notNull(dbMediaSource);
		Cluster cluster = null;
		String[] ips = StringUtils.split(dbMediaSource.getUrl(), ";");
		Builder builder = Cluster.builder();
		for (String ip : ips) {
			String[] ports = StringUtils.split(ip, ":");
			if (ports.length == 2) {
				builder.addContactPoint(ports[0]).withPort(NumberUtils.toInt(ports[1], 9042));
			} else {
				builder.addContactPoint(ip);
			}
		}
		if (StringUtils.isEmpty(dbMediaSource.getUsername())) {
			cluster = builder.build();
		} else {
			cluster = builder.withCredentials(dbMediaSource.getUsername(), dbMediaSource.getPassword()).build();
		}
		Metadata metadata = cluster.getMetadata();
		logger.info("Connected to cluster: %s\n", metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			logger.info("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(),
					host.getRack());
		}
		return cluster;
	}

	/**
	 * hdfs建立config
	 * 
	 * @param dir
	 * @param ugi
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	public Configuration getConf(String dir, String ugi, String conf) throws IOException {
		URI uri = null;
		Configuration cfg = null;
		String scheme = null;
		try {
			uri = new URI(dir);
			scheme = uri.getScheme();
			if (null == scheme) {
				throw new IOException("HDFS Path missing scheme, check path begin with hdfs://ip:port/ .");
			}
			cfg = new Configuration();
			cfg.setClassLoader(DBDataSourceService.class.getClassLoader());
			if (!StringUtils.isBlank(conf) && new File(conf).exists()) {
				cfg.addResource(new Path(conf));
			}
			if (uri.getScheme() != null) {
				String fsname = String.format("%s://%s:%s", uri.getScheme(), uri.getHost(), uri.getPort());
				cfg.set("fs.default.name", fsname);
			}
			if (ugi != null) {
				cfg.set("hadoop.job.ugi", ugi);
			}
		} catch (URISyntaxException e) {
			throw new IOException(e.getMessage(), e.getCause());
		}
		return cfg;
	}

	/**
	 * 获取hdfs配置和文件目录
	 * 
	 * @param dbMediaSource
	 * @return
	 * @throws IOException
	 */
	public FileSystem getHDFS(DbMediaSource dbMediaSource) throws IOException {
		Assert.notNull(dbMediaSource);
		String[] urls = StringUtils.split(dbMediaSource.getUrl(), "||");
		FileSystem fs = FileSystem.get(getConf(urls[0], dbMediaSource.getUsername(), urls[1]));
		if (fs.exists(new Path(urls[0]))) {
			return fs;
		}
		return null;
	}

	/**
	 * 获取MetaQ的生产者对象
	 * 
	 * @param dbMediaSource
	 * @return
	 */
	public DefaultMQProducer getMQProducer(DbMediaSource dbMediaSource) {
		Assert.notNull(dbMediaSource);
		DefaultMQProducer producer = null;
		String[] urls = StringUtils.split(dbMediaSource.getUrl(), "|");
		if (urls != null && urls.length >= 2) {
			producer = new DefaultMQProducer(urls[1]);
			producer.setNamesrvAddr(urls[0]);
			producer.setInstanceName(urls[1]);
			producer.setSendMsgTimeout(3000);
			producer.setRetryTimesWhenSendFailed(3);
			try {
				producer.start();
			} catch (MQClientException e) {
				e.printStackTrace();
			}
		}
		return producer;
	}

	/**
	 * 获取kafka的生产者对象
	 * 
	 * @param dataMediaSource
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public Producer getProducer(DbMediaSource dbMediaSource) {
		Assert.notNull(dbMediaSource);
		Properties props = new Properties();
		// props.put("bootstrap.servers", dbMediaSource.getUrl());
		// props.put("zookeeper.connect", dbMediaSource.getUrl());
		String[] urls = StringUtils.split(dbMediaSource.getUrl(), "|");
		props.put("bootstrap.servers", urls[0]);
		if (urls.length == 2) {
			props.put("zookeeper.connect", urls[1]);
		}
		// props.put("metadata.broker.list", dbMediaSource.getUrl());
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("request.required.acks", "1");
		props.put("batch.size", 128000);
		props.put("buffer.memory", 97108864);
		props.put("compression.type", "gzip"); // 压缩
		props.put("producer.type", "async");
		// props.put("bootstrap.servers", dbMediaSource.getUrl());
		// props.put("acks", "all");
		// props.put("retries", 0);
		// props.put("batch.size", 16384);
		// props.put("linger.ms", 1);
		// props.put("buffer.memory", 33554432);
		// props.put("key.serializer",
		// "org.apache.kafka.common.serialization.StringSerializer");
		// props.put("value.serializer",
		// "org.apache.kafka.common.serialization.StringSerializer");
		Producer kp = new KafkaProducer(props);
		return kp;
	}

	public Connection getHBaseConnection(DbMediaSource dbMediaSource) throws IOException {
		Assert.notNull(dbMediaSource);
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(dbMediaSource.getUrl()));// ddResource(conf.getClass().getResourceAsStream("/hbase-site.xml"));
		System.setProperty("HADOOP_USER_NAME", dbMediaSource.getUsername());
		return ConnectionFactory.createConnection(conf);
	}

	/**
	 * 创建elasticSearch client
	 * 
	 * @param dataMediaSource
	 * @return
	 */
	public Client getClient(DbMediaSource dbMediaSource) {
		Assert.notNull(dbMediaSource);
		Client client = null;
		String[] urls = StringUtils.split(dbMediaSource.getUrl(), "||");
		if (urls.length != 3)
			return null;
		String[] hosts = StringUtils.split(urls[0], ";");
		int id = 0;
		InetSocketTransportAddress[] transportAddress = new InetSocketTransportAddress[hosts.length];
		for (String host : hosts) {
			String[] hp = StringUtils.split(host, ":");
			try {
				transportAddress[id] = new InetSocketTransportAddress(InetAddress.getByName(hp[0]),
						NumberUtils.toInt(hp[1], 9300));
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			id++;
		}
		Settings settings = null;
		if (StringUtils.isNotEmpty(dbMediaSource.getUsername())) {
//			settings = Settings.settingsBuilder().put("cluster.name", urls[1])
//					.put("shield.user", dbMediaSource.getUsername() + ":" + dbMediaSource.getPassword())
//					.put("client.transport.sniff", true).build();
//			client = TransportClient.builder().addPlugin(ShieldPlugin.class).addPlugin(DeleteByQueryPlugin.class)
//					.settings(settings).build().addTransportAddresses(transportAddress);
		} else {
			settings = Settings.settingsBuilder().put("cluster.name", urls[1]).put("client.transport.sniff", true)
					.build();
			client = TransportClient.builder().addPlugin(DeleteByQueryPlugin.class).settings(settings).build()
					.addTransportAddresses(transportAddress);
		}
		IndicesExistsResponse response = client.admin().indices().prepareExists(urls[2]).execute().actionGet();
		if (!response.isExists()) {
			return null;
		}
		return client;
	}

	/**
	 * 关闭各种连接
	 */
	public void destroy(Long pipelineId) {
		try {
			LoadingCache<DbMediaSource, Object> sources = dataSources.get(pipelineId);
			// invalidate(pipelineId);
			if (sources != null) {
				for (Object dbconn : sources.asMap().values()) {
					try {
						if (dbconn instanceof DataSource) {
							DataSource source = (DataSource) dbconn;
							// for filter to destroy custom datasource
							if (letHandlerDestroyIfSupport(pipelineId, source)) {
								continue;
							} // fallback for regular destroy TODO need to
								// integrate to handler
							BasicDataSource basicDataSource = (BasicDataSource) source;
							basicDataSource.close();
						} else if (dbconn instanceof Client) {
							Client client = (Client) dbconn;
							client.close();
						} else if (dbconn instanceof Connection) {
							Connection hbaseconn = (Connection) dbconn;
							hbaseconn.close();
						} else if (dbconn instanceof Producer) {
							Producer producer = (Producer) dbconn;
							producer.close();
						} else if (dbconn instanceof Cluster) {
							Cluster cluster = (Cluster) dbconn;
							cluster.close();
						} else if (dbconn instanceof FileSystem) {
							FileSystem filesystem = (FileSystem) dbconn;
							filesystem.close();
						}
					} catch (SQLException e) {
						logger.error("ERROR ## close the datasource has an error", e);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				sources.invalidateAll();
				sources.cleanUp();
			}
		} catch (ExecutionException e1) {
			e1.printStackTrace();

		}
	}

	private boolean letHandlerDestroyIfSupport(Long pipelineId, DataSource dataSource) {
		boolean destroied = false;

		if (CollectionUtils.isEmpty(this.dataSourceHandlers)) {
			return destroied;
		}
		for (DataSourceHanlder handler : this.dataSourceHandlers) {
			if (handler.support(dataSource)) {
				handler.destory(pipelineId);
				destroied = true;
				return destroied;
			}
		}
		return destroied;

	}

	public void destroy() throws Exception {
		dataSources.invalidateAll();
		dataSources.cleanUp();

		// for (Long pipelineId : dataSources.keySet()) {
		// destroy(pipelineId);
		// }
	}

	private DataSource createDataSource(String url, String userName, String password, String driverClassName,
			DataMediaType dataMediaType, String encoding) {
		BasicDataSource dbcpDs = new BasicDataSource();
		dbcpDs.setInitialSize(initialSize);// 初始化连接池时创建的连接数
		if (dataMediaType.isGreenPlum()) {
			dbcpDs.setMaxActive(maxActive * 2);// 连接池允许的最大并发连接数，值为非正数时表示不限制
			dbcpDs.setMaxIdle(maxIdle * 2);// 连接池中的最大空闲连接数，超过时，多余的空闲连接将会被释放，值为负数时表示不限制
		} else {
			dbcpDs.setMaxActive(maxActive);// 连接池允许的最大并发连接数，值为非正数时表示不限制
			dbcpDs.setMaxIdle(maxIdle);// 连接池中的最大空闲连接数，超过时，多余的空闲连接将会被释放，值为负数时表示不限制
		}
		dbcpDs.setMinIdle(minIdle);// 连接池中的最小空闲连接数，低于此数值时将会创建所欠缺的连接，值为0时表示不创建
		dbcpDs.setMaxWait(maxWait);// 以毫秒表示的当连接池中没有可用连接时等待可用连接返回的时间，超时则抛出异常，值为-1时表示无限等待
		dbcpDs.setRemoveAbandoned(true);// 是否清除已经超过removeAbandonedTimeout设置的无效连接
		dbcpDs.setLogAbandoned(true);// 当清除无效链接时是否在日志中记录清除信息的标志
		dbcpDs.setRemoveAbandonedTimeout(removeAbandonedTimeout); // 以秒表示清除无效链接的时限
		dbcpDs.setNumTestsPerEvictionRun(numTestsPerEvictionRun);// 确保连接池中没有已破损的连接
		dbcpDs.setTestOnBorrow(false);// 指定连接被调用时是否经过校验
		dbcpDs.setTestOnReturn(false);// 指定连接返回到池中时是否经过校验
		dbcpDs.setTestWhileIdle(true);// 指定连接进入空闲状态时是否经过空闲对象驱逐进程的校验
		dbcpDs.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis); // 以毫秒表示空闲对象驱逐进程由运行状态进入休眠状态的时长，值为非正数时表示不运行任何空闲对象驱逐进程
		dbcpDs.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis); // 以毫秒表示连接被空闲对象驱逐进程驱逐前在池中保持空闲状态的最小时间

		// 动态的参数
		dbcpDs.setDriverClassName(driverClassName);
		dbcpDs.setUrl(url);
		dbcpDs.setUsername(userName);
		dbcpDs.setPassword(password);

		if (dataMediaType.isOracle()) {
			dbcpDs.addConnectionProperty("restrictGetTables", "true");
			dbcpDs.setValidationQuery("select 1 from dual");
		} else if (dataMediaType.isMysql()) {
			// open the batch mode for mysql since 5.1.8
			dbcpDs.addConnectionProperty("useServerPrepStmts", "false");
			dbcpDs.addConnectionProperty("rewriteBatchedStatements", "true");
			dbcpDs.addConnectionProperty("zeroDateTimeBehavior", "convertToNull");// 将0000-00-00的时间类型返回null
			dbcpDs.addConnectionProperty("yearIsDateType", "false");// 直接返回字符串，不做year转换date处理
			dbcpDs.addConnectionProperty("noDatetimeStringSync", "true");// 返回时间类型的字符串,不做时区处理
			if (StringUtils.isNotEmpty(encoding)) {
				if (StringUtils.equalsIgnoreCase(encoding, "utf8mb4")) {
					dbcpDs.addConnectionProperty("characterEncoding", "utf8");
					dbcpDs.setConnectionInitSqls(Arrays.asList("set names utf8mb4"));
				} else {
					dbcpDs.addConnectionProperty("characterEncoding", encoding);
				}
			}
			dbcpDs.setValidationQuery("select 1");
		} else {
			logger.error("ERROR ## Unknow database type");
		}

		return dbcpDs;
	}

	/**
	 * 扩展功能,可以自定义一些自己实现的 dataSource
	 */
	private DataSource preCreate(Long pipelineId, DbMediaSource dbMediaSource) {

		if (CollectionUtils.isEmpty(dataSourceHandlers)) {
			return null;
		}

		DataSource dataSource = null;
		for (DataSourceHanlder handler : dataSourceHandlers) {
			if (handler.support(dbMediaSource)) {
				dataSource = handler.create(pipelineId, dbMediaSource);
				if (dataSource != null) {
					return dataSource;
				}
			}
		}
		return null;
	}

	public void setMaxWait(int maxWait) {
		this.maxWait = maxWait;
	}

	public void setMinIdle(int minIdle) {
		this.minIdle = minIdle;
	}

	public void setInitialSize(int initialSize) {
		this.initialSize = initialSize;
	}

	public void setMaxActive(int maxActive) {
		this.maxActive = maxActive;
	}

	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
		this.numTestsPerEvictionRun = numTestsPerEvictionRun;
	}

	public void setTimeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis) {
		this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
	}

	public void setRemoveAbandonedTimeout(int removeAbandonedTimeout) {
		this.removeAbandonedTimeout = removeAbandonedTimeout;
	}

	public void setMinEvictableIdleTimeMillis(int minEvictableIdleTimeMillis) {
		this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
	}

	public void setDataSourceHandlers(List<DataSourceHanlder> dataSourceHandlers) {
		this.dataSourceHandlers = dataSourceHandlers;
	}

	@Override
	public void destroy(Long pipelineId, DbMediaSource source) {
		try {
			LoadingCache<DbMediaSource, Object> sources = dataSources.get(pipelineId);
			// invalidate(pipelineId);
			if (sources != null) {
				Object dbconn = sources.get(source);
				try {
					if (dbconn instanceof DataSource) {
						DataSource dsource = (DataSource) dbconn;
						// for filter to destroy custom datasource
						if (!letHandlerDestroyIfSupport(pipelineId, dsource)) {
							BasicDataSource basicDataSource = (BasicDataSource) dsource;
							basicDataSource.close();
						}
					} else if (dbconn instanceof Client) {
						Client client = (Client) dbconn;
						client.close();
					} else if (dbconn instanceof Connection) {
						Connection hbaseconn = (Connection) dbconn;
						hbaseconn.close();
					} else if (dbconn instanceof Producer) {
						Producer producer = (Producer) dbconn;
						producer.close();
					} else if (dbconn instanceof Cluster) {
						Cluster cluster = (Cluster) dbconn;
						cluster.close();
					} else if (dbconn instanceof FileSystem) {
						FileSystem filesystem = (FileSystem) dbconn;
						filesystem.close();
					}
				} catch (SQLException e) {
					logger.error("ERROR ## close the datasource has an error", e);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					sources.invalidate(source);
				}
			}
		} catch (ExecutionException e1) {
			e1.printStackTrace();
		}

	}
}
