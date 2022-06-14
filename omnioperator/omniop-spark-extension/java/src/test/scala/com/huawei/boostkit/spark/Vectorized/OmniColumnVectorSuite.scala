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

package com.huawei.boostkit.spark.vectorized

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.types._

class OmniColumnVectorSuite extends SparkFunSuite {
  test("int") {
    val schema = new StructType().add("int", IntegerType);
    val vectors: Seq[OmniColumnVector] = OmniColumnVector.allocateColumns(4, schema, true)
    vectors(0).putInt(0, 1)
    vectors(0).putInt(1, 2)
    vectors(0).putInt(2, 3)
    vectors(0).putInt(3, 4)
    assert(1 == vectors(0).getInt(0))
    assert(2 == vectors(0).getInt(1))
    assert(3 == vectors(0).getInt(2))
    assert(4 == vectors(0).getInt(3))
    vectors(0).close()
  }
}
