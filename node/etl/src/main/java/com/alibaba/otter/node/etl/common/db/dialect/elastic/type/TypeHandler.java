/*
 * frxs Inc.  湖南兴盛优选电子商务有限公司.
 * Copyright (c) 2017-2019. All Rights Reserved.
 */
package com.alibaba.otter.node.etl.common.db.dialect.elastic.type;

import java.util.HashMap;
import java.util.Map;

/**
 * @author weishuichao
 * @version $Id: TypeHandler.java,v 0.1 2019年11月04日 9:32 $Exp
 */
public interface TypeHandler {


    Object  convert(Integer type,String value);

}