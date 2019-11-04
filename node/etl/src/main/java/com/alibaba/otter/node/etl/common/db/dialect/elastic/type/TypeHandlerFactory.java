/*
 * frxs Inc.  湖南兴盛优选电子商务有限公司.
 * Copyright (c) 2017-2019. All Rights Reserved.
 */
package com.alibaba.otter.node.etl.common.db.dialect.elastic.type;

import com.alibaba.otter.node.etl.common.db.dialect.elastic.ElasticSearchDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author weishuichao
 * @version $Id: TypeHandlerFactory.java,v 0.1 2019年11月04日 9:44 $Exp
 */
public class TypeHandlerFactory {
    protected static final Logger logger = LoggerFactory.getLogger(TypeHandlerFactory.class);

    private static Map<Integer,TypeHandler> handlerMap = new HashMap<Integer, TypeHandler>();
    static  {

        /**
         *         registerJdbcType(2003, "ARRAY", JdbcTypeCategoryEnum.SPECIAL);
         *         registerJdbcType(-5, "BIGINT", JdbcTypeCategoryEnum.NUMERIC);
         *         registerJdbcType(-2, "BINARY", JdbcTypeCategoryEnum.BINARY);
         *         registerJdbcType(-7, "BIT", JdbcTypeCategoryEnum.NUMERIC);
         *         registerJdbcType(2004, "BLOB", JdbcTypeCategoryEnum.BINARY);
         *         registerJdbcType(1, "CHAR", JdbcTypeCategoryEnum.TEXTUAL);
         *         registerJdbcType(2005, "CLOB", JdbcTypeCategoryEnum.TEXTUAL);
         *         registerJdbcType(91, "DATE", JdbcTypeCategoryEnum.DATETIME);
         *         registerJdbcType(3, "DECIMAL", JdbcTypeCategoryEnum.NUMERIC);
         *         registerJdbcType(2001, "DISTINCT", JdbcTypeCategoryEnum.SPECIAL);
         *         registerJdbcType(8, "DOUBLE", JdbcTypeCategoryEnum.NUMERIC);
         *         registerJdbcType(6, "FLOAT", JdbcTypeCategoryEnum.NUMERIC);
         *         registerJdbcType(4, "INTEGER", JdbcTypeCategoryEnum.NUMERIC);
         *         registerJdbcType(2000, "JAVA_OBJECT", JdbcTypeCategoryEnum.SPECIAL);
         *         registerJdbcType(-4, "LONGVARBINARY", JdbcTypeCategoryEnum.BINARY);
         *         registerJdbcType(-1, "LONGVARCHAR", JdbcTypeCategoryEnum.TEXTUAL);
         *         registerJdbcType(0, "NULL", JdbcTypeCategoryEnum.SPECIAL);
         *         registerJdbcType(2, "NUMERIC", JdbcTypeCategoryEnum.NUMERIC);
         *         registerJdbcType(1111, "OTHER", JdbcTypeCategoryEnum.SPECIAL);
         *         registerJdbcType(7, "REAL", JdbcTypeCategoryEnum.NUMERIC);
         *         registerJdbcType(2006, "REF", JdbcTypeCategoryEnum.SPECIAL);
         *         registerJdbcType(5, "SMALLINT", JdbcTypeCategoryEnum.NUMERIC);
         *         registerJdbcType(2002, "STRUCT", JdbcTypeCategoryEnum.SPECIAL);
         *         registerJdbcType(92, "TIME", JdbcTypeCategoryEnum.DATETIME);
         *         registerJdbcType(93, "TIMESTAMP", JdbcTypeCategoryEnum.DATETIME);
         *         registerJdbcType(-6, "TINYINT", JdbcTypeCategoryEnum.NUMERIC);
         *         registerJdbcType(-3, "VARBINARY", JdbcTypeCategoryEnum.BINARY);
         *         registerJdbcType(12, "VARCHAR", JdbcTypeCategoryEnum.TEXTUAL);
         */
        handlerMap.put(-5,new LongTypeHandler());
        handlerMap.put(-7,new IntegerTypeHandler());
        handlerMap.put(1,new StringTypeHandler());
        handlerMap.put(91,new DateTimeTypeHandler());
        handlerMap.put(3,new DoubleTypeHandler());
        handlerMap.put(-5,new LongTypeHandler());
        handlerMap.put(8,new DoubleTypeHandler());
        handlerMap.put(6,new FloatTypeHandler());
        handlerMap.put(4,new IntegerTypeHandler());
        handlerMap.put(0,new NullTypeHandler());
        handlerMap.put(2,new DoubleTypeHandler());
        handlerMap.put(7,new DoubleTypeHandler());
        handlerMap.put(5,new IntegerTypeHandler());
        handlerMap.put(92,new TimeTypeHandler());
        handlerMap.put(93,new DateTimeTypeHandler());
        handlerMap.put(-6,new IntegerTypeHandler());
        handlerMap.put(12,new StringTypeHandler());
    }

    public static Object convert(Integer type,String value){
        TypeHandler handler = handlerMap.get(type);
        if(handler ==null){
            logger.warn("do not find type handler,type:"+type+";value:"+value);
            return value;
        }
        return handler.convert(type,value);

    }
}