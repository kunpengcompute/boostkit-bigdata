/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.omnidata;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.huawei.boostkit.omnidata.decode.type.*;
import com.huawei.boostkit.omnidata.model.Column;
import com.huawei.boostkit.omnidata.model.Predicate;

import io.airlift.slice.Slices;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * TestOmniDataUtils
 *
 * @since 2022-08-24
 */
public class TestOmniDataUtils {
    @Test
    public void testTransOmniDataConstantExpr() {
        String bigintValue = "9223372036854775807";
        assertEquals(OmniDataUtils.transOmniDataConstantExpr(bigintValue, BIGINT),
                new ConstantExpression(9223372036854775807L, BIGINT));

        String integerValue = "2147483647";
        assertEquals(OmniDataUtils.transOmniDataConstantExpr(integerValue, INTEGER),
                new ConstantExpression(2147483647L, INTEGER));

        String tinyintValue = "127";
        assertEquals(OmniDataUtils.transOmniDataConstantExpr(tinyintValue, TINYINT),
                new ConstantExpression(127L, TINYINT));

        String smallintValue = "32767";
        assertEquals(OmniDataUtils.transOmniDataConstantExpr(smallintValue, SMALLINT),
                new ConstantExpression(32767L, SMALLINT));

        String booleanValue = "true";
        assertEquals(OmniDataUtils.transOmniDataConstantExpr(booleanValue, BOOLEAN),
                new ConstantExpression(true, BOOLEAN));

        String dateValue = "2022-03-04";
        assertEquals(OmniDataUtils.transOmniDataConstantExpr(dateValue, DATE), new ConstantExpression(19055L, DATE));

        String doubleValue = "5.5555";
        assertEquals(OmniDataUtils.transOmniDataConstantExpr(doubleValue, DOUBLE),
                new ConstantExpression(5.5555, DOUBLE));

        String realValue = "5.5555";
        assertEquals(OmniDataUtils.transOmniDataConstantExpr(realValue, REAL),
                new ConstantExpression(1085392552L, REAL));

        String varcharValue = "omnidata";
        assertEquals(OmniDataUtils.transOmniDataConstantExpr(varcharValue, VARCHAR),
                new ConstantExpression(Slices.utf8Slice(varcharValue), VARCHAR));

        String nullValue = "null";
        assertEquals(OmniDataUtils.transOmniDataConstantExpr(nullValue, BIGINT), new ConstantExpression(null, BIGINT));
    }

    @Test
    public void testTransOmniDataType() {
        Type bigintType = OmniDataUtils.transOmniDataType("bigint");
        assertEquals(bigintType, BIGINT);

        Type longType = OmniDataUtils.transOmniDataType("long");
        assertEquals(longType, BIGINT);

        Type booleanType = OmniDataUtils.transOmniDataType("boolean");
        assertEquals(booleanType, BOOLEAN);

        Type byteType = OmniDataUtils.transOmniDataType("byte");
        assertEquals(byteType, TINYINT);

        Type tinyintType = OmniDataUtils.transOmniDataType("tinyint");
        assertEquals(tinyintType, TINYINT);

        Type char16Type = OmniDataUtils.transOmniDataType("char(16)");
        assertEquals(char16Type, CharType.createCharType(16));

        Type stringType = OmniDataUtils.transOmniDataType("string");
        assertEquals(stringType, VARCHAR);

        Type varchar128Type = OmniDataUtils.transOmniDataType("varchar(128)");
        assertEquals(varchar128Type, VARCHAR);

        Type doubleType = OmniDataUtils.transOmniDataType("double");
        assertEquals(doubleType, DOUBLE);

        Type dateType = OmniDataUtils.transOmniDataType("date");
        assertEquals(dateType, DATE);

        Type floatType = OmniDataUtils.transOmniDataType("float");
        assertEquals(floatType, REAL);

        Type intType = OmniDataUtils.transOmniDataType("int");
        assertEquals(intType, INTEGER);

        Type integerType = OmniDataUtils.transOmniDataType("integer");
        assertEquals(integerType, INTEGER);

        Type shortType = OmniDataUtils.transOmniDataType("short");
        assertEquals(shortType, SMALLINT);

        Type smallintType = OmniDataUtils.transOmniDataType("smallint");
        assertEquals(smallintType, SMALLINT);

        Type type = OmniDataUtils.transOmniDataType("array<string>");
        assertEquals(type, new ArrayType<>(VARCHAR));
    }

    @Test
    public void testTransOmniDataDecodeType() {
        DecodeType bigintDecode = OmniDataUtils
                .transOmniDataDecodeType("bigint");
        assertEquals(bigintDecode.getClass(), LongDecodeType.class);

        DecodeType booleanDecode = OmniDataUtils
                .transOmniDataDecodeType("boolean");
        assertEquals(booleanDecode.getClass(), BooleanDecodeType.class);

        DecodeType byteDecode = OmniDataUtils
                .transOmniDataDecodeType("byte");
        assertEquals(byteDecode.getClass(), ByteDecodeType.class);

        DecodeType tinyintDecode = OmniDataUtils
                .transOmniDataDecodeType("tinyint");
        assertEquals(tinyintDecode.getClass(), ByteDecodeType.class);

        DecodeType charDecode = OmniDataUtils
                .transOmniDataDecodeType("char(16)");
        assertEquals(charDecode.getClass(), VarcharDecodeType.class);

        DecodeType stringDecode = OmniDataUtils
                .transOmniDataDecodeType("string");
        assertEquals(stringDecode.getClass(), VarcharDecodeType.class);

        DecodeType varcharDecode = OmniDataUtils
                .transOmniDataDecodeType("varchar(128)");
        assertEquals(varcharDecode.getClass(), VarcharDecodeType.class);

        DecodeType dateDecode = OmniDataUtils
                .transOmniDataDecodeType("date");
        assertEquals(dateDecode.getClass(), DateDecodeType.class);

        DecodeType doubleDecode = OmniDataUtils
                .transOmniDataDecodeType("double");
        assertEquals(doubleDecode.getClass(), DoubleDecodeType.class);

        DecodeType floatDecode = OmniDataUtils
                .transOmniDataDecodeType("float");
        assertEquals(floatDecode.getClass(), FloatDecodeType.class);

        DecodeType realDecode = OmniDataUtils
                .transOmniDataDecodeType("real");
        assertEquals(realDecode.getClass(), FloatDecodeType.class);

        DecodeType intDecode = OmniDataUtils
                .transOmniDataDecodeType("int");
        assertEquals(intDecode.getClass(), IntDecodeType.class);

        DecodeType integerDecode = OmniDataUtils
                .transOmniDataDecodeType("integer");
        assertEquals(integerDecode.getClass(), IntDecodeType.class);

        DecodeType smallintDecode = OmniDataUtils
                .transOmniDataDecodeType("smallint");
        assertEquals(smallintDecode.getClass(), ShortDecodeType.class);

        try {
            OmniDataUtils.transOmniDataDecodeType("test");
        } catch (UnsupportedOperationException e) {
            assertEquals(e.getMessage(), "OmniData Hive unsupported this type:test");
        }
    }

    @Test
    public void testStripEnd() {
        String str = OmniDataUtils.stripEnd("value    ", " ");
        assertEquals(str, "value");
    }

    @Test
    public void testTransOmniDataAggDecodeType() {
        DecodeType bigintDecode = OmniDataUtils
                .transOmniDataAggDecodeType("bigint");
        assertEquals(bigintDecode.getClass(), LongDecodeType.class);

        DecodeType booleanDecode = OmniDataUtils
                .transOmniDataAggDecodeType("boolean");
        assertEquals(booleanDecode.getClass(), BooleanDecodeType.class);

        DecodeType byteDecode = OmniDataUtils
                .transOmniDataAggDecodeType("byte");
        assertEquals(byteDecode.getClass(), LongToByteDecodeType.class);

        DecodeType tinyintDecode = OmniDataUtils
                .transOmniDataAggDecodeType("tinyint");
        assertEquals(tinyintDecode.getClass(), LongToByteDecodeType.class);

        DecodeType charDecode = OmniDataUtils
                .transOmniDataAggDecodeType("char(16)");
        assertEquals(charDecode.getClass(), VarcharDecodeType.class);

        DecodeType stringDecode = OmniDataUtils
                .transOmniDataAggDecodeType("string");
        assertEquals(stringDecode.getClass(), VarcharDecodeType.class);

        DecodeType varcharDecode = OmniDataUtils
                .transOmniDataAggDecodeType("varchar(128)");
        assertEquals(varcharDecode.getClass(), VarcharDecodeType.class);

        DecodeType dateDecode = OmniDataUtils
                .transOmniDataAggDecodeType("date");
        assertEquals(dateDecode.getClass(), LongToIntDecodeType.class);

        DecodeType doubleDecode = OmniDataUtils
                .transOmniDataAggDecodeType("double");
        assertEquals(doubleDecode.getClass(), DoubleDecodeType.class);

        DecodeType floatDecode = OmniDataUtils
                .transOmniDataAggDecodeType("float");
        assertEquals(floatDecode.getClass(), LongToFloatDecodeType.class);

        DecodeType realDecode = OmniDataUtils
                .transOmniDataAggDecodeType("real");
        assertEquals(realDecode.getClass(), LongToFloatDecodeType.class);

        DecodeType intDecode = OmniDataUtils
                .transOmniDataAggDecodeType("int");
        assertEquals(intDecode.getClass(), LongToIntDecodeType.class);

        DecodeType integerDecode = OmniDataUtils
                .transOmniDataAggDecodeType("integer");
        assertEquals(integerDecode.getClass(), LongToIntDecodeType.class);

        DecodeType smallintDecode = OmniDataUtils
                .transOmniDataAggDecodeType("smallint");
        assertEquals(smallintDecode.getClass(), LongToShortDecodeType.class);

        try {
            OmniDataUtils.transOmniDataAggDecodeType("test");
        } catch (UnsupportedOperationException e) {
            assertEquals(e.getMessage(), "OmniData Hive unsupported this type:test");
        }
    }

    @Test
    public void testTransOperator() {
        String lessThan = OmniDataUtils.transOmniDataOperator("LESS_THAN");
        assertEquals(lessThan, "less_than");

        String lessThanEquals = OmniDataUtils.transOmniDataOperator("LESS_THAN_EQUALS");
        assertEquals(lessThanEquals, "less_than_or_equal");

        String equals = OmniDataUtils.transOmniDataOperator("EQUALS");
        assertEquals(equals, "equal");

        String test = OmniDataUtils.transOmniDataOperator("test");
        assertEquals(test, "test");
    }

    @Test
    public void testTransAggType() {
        assertEquals(OmniDataUtils.transAggType(INTEGER), BIGINT);
        assertEquals(OmniDataUtils.transAggType(REAL), DOUBLE);
        assertEquals(OmniDataUtils.transAggType(VARCHAR), VARCHAR);
    }

    @Test
    public void testTransOmniDataUdfType() {
        CharTypeInfo charTypeInfo = new CharTypeInfo(11);
        assertEquals(OmniDataUtils.transOmniDataUdfType(charTypeInfo),
                CharType.createCharType(charTypeInfo.getLength()));
        VarcharTypeInfo varcharTypeInfo = new VarcharTypeInfo(128);
        assertEquals(OmniDataUtils.transOmniDataUdfType(varcharTypeInfo),
                VarcharType.createVarcharType(varcharTypeInfo.getLength()));
    }

    @Test
    public void testGetValueForPartitionKey() {
        List<Column> columns = new ArrayList<>();
        Column normalColumn = new Column(0, "normal", INTEGER);
        Column partitionColumn = new Column(1, "partition", INTEGER, true, null);
        columns.add(normalColumn);
        columns.add(partitionColumn);

        Predicate oldPredicate = new Predicate(new ArrayList<>(), columns, Optional.empty(), new ArrayList<>(),
                new HashMap<>(), new HashMap<>(), Optional.empty(), OptionalLong.empty());

        NdpPredicateInfo predicateInfo1 = new NdpPredicateInfo(true, true, true, true, oldPredicate, new ArrayList<>(),
                new ArrayList<>(), new ArrayList<>(), null, "orc");

        NdpPredicateInfo predicateInfo2 = new NdpPredicateInfo(true, true, true, true, oldPredicate, new ArrayList<>(),
                new ArrayList<>(), new ArrayList<>(), null, "orc");

        String path1 = "/test/partition=100";
        String path2 = "/test/partition=default";

        OmniDataUtils.addPartitionValues(predicateInfo1, path1, "default");
        assertTrue(predicateInfo1.getPredicate().getColumns().get(1).isPartitionKey());
        assertEquals(predicateInfo1.getPredicate().getColumns().get(1).getPartitionKeyValue(), "100");

        OmniDataUtils.addPartitionValues(predicateInfo2, path2, "default");
        assertTrue(predicateInfo2.getPredicate().getColumns().get(1).isPartitionKey());
        assertNull(predicateInfo2.getPredicate().getColumns().get(1).getPartitionKeyValue());
    }
}