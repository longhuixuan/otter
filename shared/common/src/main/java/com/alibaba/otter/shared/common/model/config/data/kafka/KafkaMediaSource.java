package com.alibaba.otter.shared.common.model.config.data.kafka;

import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;

/**
 * kafka 连接参数
 * 
 * @author zhangxhui joezxh@qq.com
 * 
 *         date: 2017-02-10
 */
public class KafkaMediaSource extends DataMediaSource {

	private static final long serialVersionUID = 1966567625036110432L;

	private String bootstrapServers;
	private String zookeeperConnect;
	private long batchSize = 128000L;
	private long bufferMemory = 97108864L;

	public static void main(String[] args) {
		KafkaMediaSource kms=new KafkaMediaSource();
		kms.setBatchSize(1000L);
		kms.setBootstrapServers("127.0.0.1:2716");
		kms.setBufferMemory(1024000L);
		kms.setZookeeperConnect("127.0.0.1:2181");
		System.out.println(JSON.toJSONString(kms));
	}

	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}

	public String getZookeeperConnect() {
		return zookeeperConnect;
	}

	public void setZookeeperConnect(String zookeeperConnect) {
		this.zookeeperConnect = zookeeperConnect;
	}

	public long getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(long batchSize) {
		this.batchSize = batchSize;
	}

	public long getBufferMemory() {
		return bufferMemory;
	}

	public void setBufferMemory(long bufferMemory) {
		this.bufferMemory = bufferMemory;
	}

}
