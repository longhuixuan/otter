/*
 * frxs Inc.  湖南兴盛优选电子商务有限公司.
 * Copyright (c) 2017-2019. All Rights Reserved.
 */
package com.alibaba.otter.node.extend.processor;

import com.alibaba.otter.shared.etl.model.EventData;
import com.alibaba.otter.shared.etl.model.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * 忽略删除操作的数据同步
 * @author weishuichao
 * @version $Id: IgnoreDeleteEventProcessor.java,v 0.1 2019年10月02日 15:22 $Exp
 */

public class IgnoreDeleteEventProcessor extends  AbstractEventProcessor {

    private Logger logger = LoggerFactory.getLogger(IgnoreDeleteEventProcessor.class);
    @Override
    public boolean process(EventData eventData) {
        logger.info("IgnoreDeleteEventProcessor");
        if(eventData.getEventType()== EventType.DELETE){
            return false;
        }
        return true;
    }
}