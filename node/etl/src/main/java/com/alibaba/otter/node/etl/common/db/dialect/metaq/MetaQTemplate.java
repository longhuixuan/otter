package com.alibaba.otter.node.etl.common.db.dialect.metaq;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.node.etl.common.db.dialect.NoSqlTemplate;
import com.alibaba.otter.node.etl.common.db.utils.SqlUtils;
import com.alibaba.otter.node.etl.load.exception.ConnClosedException;
import com.alibaba.otter.shared.etl.model.EventData;

public class MetaQTemplate implements NoSqlTemplate {
	private DefaultMQProducer producer = null;
	protected static final Logger logger = LoggerFactory.getLogger(MetaQTemplate.class);

	private ObjectMapper mapper = new ObjectMapper();

	public MetaQTemplate(DefaultMQProducer producer) {
		this.producer = producer;
	}

	private EventData sendMessage(EventData event) throws ConnClosedException {
		String pkVal = SqlUtils.getPriKey(event);
		try {
			event.setLoadDataTime(System.currentTimeMillis());
			SendResult sendResult =null;
			if (event.getEventType().isDdl()) {
				Message msg = new Message(event.getSchemaName() + "_DDL",// topic
	                    event.getTableName(),// tag
	                    pkVal,// key
	                    (mapper.writeValueAsString(event)).getBytes());// body
	            sendResult = producer.send(msg);
			}else{
				Message msg = new Message(event.getSchemaName(),// topic
	                    event.getTableName(),// tag
	                    pkVal,// key
	                    (mapper.writeValueAsString(event)).getBytes());// body
	            sendResult = producer.send(msg);
			}
			logger.debug(sendResult.toString()+ " MetaQ消息发送完毕时间: event time:" + new Timestamp(event.getExecuteTime()) + "  当前时间:"
					+ new Timestamp(System.currentTimeMillis()));
			event.setExeResult(sendResult.toString());
		}  catch (IOException e) {
			throw new ConnClosedException(e.getMessage());
		} catch (MQClientException e) {
			throw new ConnClosedException(e.getMessage());
		} catch (RemotingException e) {
			throw new ConnClosedException(e.getMessage());
		} catch (MQBrokerException e) {
			throw new ConnClosedException(e.getMessage());
		} catch (InterruptedException e) {
			throw new ConnClosedException(e.getMessage());
		}
		return event;
	}

	@Override
	public List<EventData> batchEventDatas(List<EventData> events) throws ConnClosedException {
		for(EventData event:events){
			sendMessage(event);
		}
		return events;
	}

	@Override
	public void distory() throws ConnClosedException {
		
	}

	@Override
	public EventData insertEventData(EventData event) throws ConnClosedException {
		return sendMessage(event);
	}

	@Override
	public EventData updateEventData(EventData event) throws ConnClosedException {
		return sendMessage(event);
	}

	@Override
	public EventData deleteEventData(EventData event) throws ConnClosedException {
		return sendMessage(event);
	}

	@Override
	public EventData createTable(EventData event) throws ConnClosedException {
		return sendMessage(event);
	}

	@Override
	public EventData alterTable(EventData event) throws ConnClosedException {
		return sendMessage(event);
	}

	@Override
	public EventData eraseTable(EventData event) throws ConnClosedException {
		return sendMessage(event);
	}

	@Override
	public EventData truncateTable(EventData event) throws ConnClosedException {
		return sendMessage(event);
	}

	@Override
	public EventData renameTable(EventData event) throws ConnClosedException {
		return sendMessage(event);
	}

}
