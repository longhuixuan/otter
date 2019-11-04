/*
 * frxs Inc.  湖南兴盛优选电子商务有限公司.
 * Copyright (c) 2017-2019. All Rights Reserved.
 */
package com.alibaba.otter.node.etl.common.db.dialect.elastic.type;

import org.apache.commons.lang.StringUtils;

/**
 * @author weishuichao
 * @version $Id: BooleanTypeHandler.java,v 0.1 2019年11月04日 9:39 $Exp
 */
public class StringTypeHandler implements  TypeHandler{
    @Override
    public Object convert(Integer type, String value) {
       return value;
    }
}