/*
 * Copyright (C) 2022-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.vectorized;

import junit.framework.TestCase;
import nova.hetu.omniruntime.vector.*;
import org.apache.orc.Reader.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.execution.datasources.orc.OrcColumnarNativeReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.validation.constraints.AssertTrue;

import static org.junit.Assert.*;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class OmniColumnVectorTest extends TestCase {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        System.out.println("OmniColumnVectorTest test finished");
    }


    @Test
    public void testNewOmniColumnVector() {
        OmniColumnVector vecTmp = new OmniColumnVector(4096, DataTypes.LongType, true);
        LongVec vecLong = new LongVec(4096);
        vecTmp.setVec(vecLong);
        vecTmp.putLong(0, 123L);
        assertTrue(vecTmp.getLong(0) == 123L);
        assertTrue(vecTmp.getVec() != null);
        vecTmp.close();

        OmniColumnVector vecTmp1 = new OmniColumnVector(4096, DataTypes.IntegerType, true);
        IntVec vecInt = new IntVec(4096);
        vecTmp1.setVec(vecInt);
        vecTmp1.putInt(0, 123);
        assertTrue(vecTmp1.getInt(0) == 123);
        assertTrue(vecTmp1.getVec() != null);
        vecTmp1.close();

        OmniColumnVector vecTmp3 = new OmniColumnVector(4096, DataTypes.BooleanType, true);
        BooleanVec vecBoolean = new BooleanVec(4096);
        vecTmp3.setVec(vecBoolean);
        vecTmp3.putBoolean(0, true);
        assertTrue(vecTmp3.getBoolean(0) == true);
        assertTrue(vecTmp3.getVec() != null);
        vecTmp3.close();

        OmniColumnVector vecTmp4 = new OmniColumnVector(4096, DataTypes.BooleanType, false);
        BooleanVec vecBoolean1 = new BooleanVec(4096);
        vecTmp4.setVec(vecBoolean1);
        vecTmp4.putBoolean(0, true);
        assertTrue(vecTmp4.getBoolean(0) == true);
        assertTrue(vecTmp4.getVec() != null);
        vecTmp4.close();
    }

    @Test
    public void testGetsPuts() {
        OmniColumnVector vecTmp = new OmniColumnVector(4096, DataTypes.LongType, true);
        LongVec vecLong = new LongVec(4096);
        vecTmp.setVec(vecLong);
        vecTmp.putLongs(0, 10, 123L);
        long[] gets = vecTmp.getLongs(0, 10);
        for (long i : gets) {
            assertTrue(i == 123L);
        }
        assertTrue(vecTmp.getVec() != null);
        vecTmp.close();

        OmniColumnVector vecTmp1 = new OmniColumnVector(4096, DataTypes.IntegerType, true);
        IntVec vecInt = new IntVec(4096);
        vecTmp1.setVec(vecInt);
        vecTmp1.putInts(0, 10, 123);
        int[] getInts = vecTmp1.getInts(0, 10);
        for (int i : getInts) {
            assertTrue(i == 123);
        }
        assertTrue(vecTmp1.getVec() != null);
        vecTmp1.close();

        OmniColumnVector vecTmp3 = new OmniColumnVector(4096, DataTypes.BooleanType, true);
        BooleanVec vecBoolean = new BooleanVec(4096);
        vecTmp3.setVec(vecBoolean);
        vecTmp3.putBooleans(0, 10, true);
        boolean[] getBools = vecTmp3.getBooleans(0, 10);
        for (boolean i : getBools) {
            assertTrue(i == true);
        }
        assertTrue(vecTmp3.getVec() != null);
        vecTmp3.close();

        OmniColumnVector vecTmp4 = new OmniColumnVector(4096, DataTypes.BooleanType, false);
        BooleanVec vecBoolean1 = new BooleanVec(4096);
        vecTmp4.setVec(vecBoolean1);
        vecTmp4.putBooleans(0, 10, true);
        boolean[] getBools1 = vecTmp4.getBooleans(0, 10);
        for (boolean i : getBools1) {
            assertTrue(i == true);
        }
        System.out.println(vecTmp4.getBoolean(0));
        assertTrue(vecTmp4.getBoolean(0) == true);
        assertTrue(vecTmp4.getVec() != null);
        vecTmp4.close();
    }

}