/*
 * frxs Inc.  湖南兴盛优选电子商务有限公司.
 * Copyright (c) 2017-2019. All Rights Reserved.
 */
package com.alibaba.otter.node.etl.common.db.dialect.elastic.type;

import org.apache.commons.lang.StringUtils;

/**
 * @author weishuichao
 * @version $Id: IntegerTypeHandler.java,v 0.1 2019年11月04日 9:36 $Exp
 */
public class ShortTypeHandler implements  TypeHandler {
    @Override
    public Object convert(Integer type, String value) {
        if(StringUtils.isEmpty(value)){
            return null;
        }
        return Short.valueOf(value);
    }
}