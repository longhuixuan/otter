/*
 * frxs Inc.  湖南兴盛优选电子商务有限公司.
 * Copyright (c) 2017-2019. All Rights Reserved.
 */
package com.alibaba.otter.node.etl.common.db.dialect.elastic.type;

/**
 * @author weishuichao
 * @version $Id: NullTypeHandler.java,v 0.1 2019年11月04日 9:42 $Exp
 */
public class NullTypeHandler implements TypeHandler {
    @Override
    public Object convert(Integer type, String value) {
        return null;
    }
}