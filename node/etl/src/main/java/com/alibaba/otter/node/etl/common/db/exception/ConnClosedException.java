/*
 * frxs Inc.  湖南兴盛优选电子商务有限公司.
 * Copyright (c) 2017-2019. All Rights Reserved.
 */
package com.alibaba.otter.node.etl.common.db.exception;

/**
 * @author weishuichao
 * @version $Id: ConnClosedException.java,v 0.1 2019年10月07日 10:18 $Exp
 */
public class ConnClosedException  extends Exception {
    private static final long serialVersionUID = -5287085089290828159L;

    public ConnClosedException(String message) {
        super(message);
    }



}