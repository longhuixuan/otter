/*
 * frxs Inc.  湖南兴盛优选电子商务有限公司.
 * Copyright (c) 2017-2019. All Rights Reserved.
 */
package com.alibaba.otter.node.etl.common.db.dialect.elastic.type;

import org.apache.commons.lang.StringUtils;

import java.math.BigDecimal;

/**
 * @author weishuichao
 * @version $Id: FloatTypeHandler.java,v 0.1 2019年11月04日 9:37 $Exp
 */
public class NumberTypeHandler implements  TypeHandler {
    @Override
    public Object convert(Integer type, String value) {
        if(StringUtils.isEmpty(value)){
            return null;
        }

        return new BigDecimal(value);
    }
}