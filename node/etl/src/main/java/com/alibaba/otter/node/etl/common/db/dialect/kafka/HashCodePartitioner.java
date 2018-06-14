package com.alibaba.otter.node.etl.common.db.dialect.kafka;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import kafka.utils.VerifiableProperties;
/**
 * Kafka分区
 * @author Administrator
 *
 */
public class HashCodePartitioner implements Partitioner {

	
	@Override
	public void configure(Map<String, ?> arg0) {
		
	}

	@Override
	public void close() {
		
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		int partition = 0;
		List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
		int numPartitions = partitions.size();
		String stringKey = (String) key;
		int offset = stringKey.lastIndexOf('.');
		if (offset > 0) {
		     partition = Integer.parseInt(stringKey.substring(offset + 1)) % numPartitions;
		}
		return partition;
	}

}
