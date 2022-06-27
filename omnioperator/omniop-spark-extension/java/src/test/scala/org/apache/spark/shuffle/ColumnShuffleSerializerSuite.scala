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

package org.apache.spark.shuffle

import java.io.FileInputStream

import com.huawei.boostkit.spark.serialize.ColumnarBatchSerializer
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch

class ColumnShuffleSerializerSuite extends SparkFunSuite with SharedSparkSession {
  private var avgBatchNumRows: SQLMetric = _
  private var outputNumRows: SQLMetric = _

  override def sparkConf: SparkConf =
    super.sparkConf
      .setAppName("test ColumnarShuffleDeSerializer")
      .set("spark.shuffle.compress", "true")
      .set("spark.io.compression.codec", "lz4")

  override def beforeEach(): Unit = {
    avgBatchNumRows = SQLMetrics.createAverageMetric(spark.sparkContext,
      "test serializer avg read batch num rows")
    outputNumRows = SQLMetrics.createAverageMetric(spark.sparkContext,
      "test serializer number of output rows")
  }

  test("columnar shuffle deserialize some row nullable value lz4 compressed") {
    val input = getTestResourcePath("test-data/shuffle_split_fixed_singlePartition_someNullRow")
    val serializer =
      new ColumnarBatchSerializer(avgBatchNumRows, outputNumRows).newInstance()
    val deserializedStream =
      serializer.deserializeStream(new FileInputStream(input))

    val kv = deserializedStream.asKeyValueIterator
    var length = 0
    kv.foreach {
      case (_, batch: ColumnarBatch) =>
        length += 1
        assert(batch.numRows == 600)
        assert(batch.numCols == 4)
        (0 until batch.numCols).foreach { i =>
          val valueVector =
            batch
              .column(i)
              .asInstanceOf[OmniColumnVector]
              .getVec
          assert(valueVector.getSize == batch.numRows)
        }
        batch.close()
    }
    assert(length == 1)
    deserializedStream.close()
  }

  test("columnar shuffle deserialize some col nullable value lz4 compressed") {
    val input = getTestResourcePath("test-data/shuffle_split_fixed_singlePartition_someNullCol")
    val serializer =
      new ColumnarBatchSerializer(avgBatchNumRows, outputNumRows).newInstance()
    val deserializedStream =
      serializer.deserializeStream(new FileInputStream(input))

    val kv = deserializedStream.asKeyValueIterator
    var length = 0
    kv.foreach {
      case (_, batch: ColumnarBatch) =>
        length += 1
        assert(batch.numRows == 600)
        assert(batch.numCols == 4)
        (0 until batch.numCols).foreach { i =>
          val valueVector =
            batch
              .column(i)
              .asInstanceOf[OmniColumnVector]
              .getVec
          assert(valueVector.getSize == batch.numRows)
        }
        batch.close()
    }
    assert(length == 1)
    deserializedStream.close()
  }
}
