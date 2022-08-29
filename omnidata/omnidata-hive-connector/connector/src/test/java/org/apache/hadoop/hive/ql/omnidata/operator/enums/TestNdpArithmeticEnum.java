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

package org.apache.hadoop.hive.ql.omnidata.operator.enums;

import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDoubleToString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColSubtractDecimalColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColAddDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColDivideDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColModuloDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColMultiplyDoubleColumn;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for NdpArithmeticEnum
 *
 * @since 2022-08-24
 */
public class TestNdpArithmeticEnum {
    @Test
    public void test() {
        // MULTIPLY
        Assert.assertEquals(NdpArithmeticEnum.MULTIPLY,
                NdpArithmeticEnum.getArithmeticByClass(DoubleColMultiplyDoubleColumn.class));

        // ADD
        Assert.assertEquals(NdpArithmeticEnum.ADD,
                NdpArithmeticEnum.getArithmeticByClass(DoubleColAddDoubleColumn.class));

        // SUBTRACT
        Assert.assertEquals(NdpArithmeticEnum.SUBTRACT,
                NdpArithmeticEnum.getArithmeticByClass(DecimalColSubtractDecimalColumn.class));

        // DIVIDE
        Assert.assertEquals(NdpArithmeticEnum.DIVIDE,
                NdpArithmeticEnum.getArithmeticByClass(DoubleColDivideDoubleColumn.class));

        // MODULO
        Assert.assertEquals(NdpArithmeticEnum.MODULUS,
                NdpArithmeticEnum.getArithmeticByClass(DoubleColModuloDoubleColumn.class));

        // CAST_IDENTITY
        Assert.assertEquals(NdpArithmeticEnum.CAST_IDENTITY,
                NdpArithmeticEnum.getArithmeticByClass(IdentityExpression.class));

        // CAST
        Assert.assertEquals(NdpArithmeticEnum.CAST, NdpArithmeticEnum.getArithmeticByClass(CastDoubleToString.class));

        // UNSUPPORTED
        Assert.assertEquals(NdpArithmeticEnum.UNSUPPORTED, NdpArithmeticEnum.getArithmeticByClass(Object.class));
    }
}