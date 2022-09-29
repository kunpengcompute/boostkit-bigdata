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

import java.io.{File, FileInputStream}

import com.huawei.boostkit.spark.serialize.ColumnarBatchSerializer
import com.huawei.boostkit.spark.vectorized.PartitionInfo
import nova.hetu.omniruntime.`type`.{DataType, _}
import nova.hetu.omniruntime.vector._
import org.apache.spark.{HashPartitioner, SparkConf, TaskContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.sort.ColumnarShuffleHandle
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Mockito.{doAnswer, when}
import org.mockito.invocation.InvocationOnMock

class ColumnShuffleSerializerLz4Suite extends SharedSparkSession {
  @Mock(answer = RETURNS_SMART_NULLS) private var taskContext: TaskContext = _
  @Mock(answer = RETURNS_SMART_NULLS) private var blockResolver: IndexShuffleBlockResolver = _
  @Mock(answer = RETURNS_SMART_NULLS) private var dependency
  : ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = _

  override def sparkConf: SparkConf =
    super.sparkConf
      .setAppName("test shuffle serializer for lz4")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.OmniColumnarShuffleManager")
      .set("spark.shuffle.compress", "true")
      .set("spark.io.compression.codec", "lz4")

  private var taskMetrics: TaskMetrics = _
  private var tempDir: File = _
  private var outputFile: File = _

  private var shuffleHandle: ColumnarShuffleHandle[Int, ColumnarBatch] = _
  private val numPartitions = 1

  protected var avgBatchNumRows: SQLMetric = _
  protected var outputNumRows: SQLMetric = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    avgBatchNumRows = SQLMetrics.createAverageMetric(spark.sparkContext,
      "test serializer avg read batch num rows")
    outputNumRows = SQLMetrics.createAverageMetric(spark.sparkContext,
      "test serializer number of output rows")

    tempDir = Utils.createTempDir()
    outputFile = File.createTempFile("shuffle", null, tempDir)
    taskMetrics = new TaskMetrics

    MockitoAnnotations.initMocks(this)

    shuffleHandle =
      new ColumnarShuffleHandle[Int, ColumnarBatch](shuffleId = 0, dependency = dependency)

    val types : Array[DataType] = Array[DataType](
      IntDataType.INTEGER,
      ShortDataType.SHORT,
      LongDataType.LONG,
      DoubleDataType.DOUBLE,
      new Decimal64DataType(18, 3),
      new Decimal128DataType(28, 11),
      VarcharDataType.VARCHAR,
      BooleanDataType.BOOLEAN)
    val inputTypes = DataTypeSerializer.serialize(types)

    when(dependency.partitioner).thenReturn(new HashPartitioner(numPartitions))
    when(dependency.serializer).thenReturn(new JavaSerializer(sparkConf))
    when(dependency.partitionInfo).thenReturn(
      new PartitionInfo("hash", numPartitions, types.length, inputTypes))
    when(dependency.dataSize)
      .thenReturn(SQLMetrics.createSizeMetric(spark.sparkContext, "data size"))
    when(dependency.bytesSpilled)
      .thenReturn(SQLMetrics.createSizeMetric(spark.sparkContext, "shuffle bytes spilled"))
    when(dependency.numInputRows)
      .thenReturn(SQLMetrics.createMetric(spark.sparkContext, "number of input rows"))
    when(dependency.splitTime)
      .thenReturn(SQLMetrics.createNanoTimingMetric(spark.sparkContext, "totaltime_split"))
    when(dependency.spillTime)
      .thenReturn(SQLMetrics.createNanoTimingMetric(spark.sparkContext, "totaltime_spill"))
    when(taskContext.taskMetrics()).thenReturn(taskMetrics)
    when(blockResolver.getDataFile(0, 0)).thenReturn(outputFile)

    doAnswer { (invocationOnMock: InvocationOnMock) =>
      val tmp = invocationOnMock.getArguments()(3).asInstanceOf[File]
      if (tmp != null) {
        outputFile.delete
        tmp.renameTo(outputFile)
      }
      null
    }.when(blockResolver)
      .writeIndexFileAndCommit(anyInt, anyLong, any(classOf[Array[Long]]), any(classOf[File]))
  }

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterEach()
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("write shuffle compress for lz4 with no null value") {
    val pidArray: Array[java.lang.Integer] = Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    val intArray: Array[java.lang.Integer] = Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
    val shortArray: Array[java.lang.Integer] = Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
    val longArray: Array[java.lang.Long] = Array(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L,
      17L, 18L, 19L, 20L)
    val doubleArray: Array[java.lang.Double] = Array(0.0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.10, 11.11, 12.12,
      13.13, 14.14, 15.15, 16.16, 17.17, 18.18, 19.19, 20.20)
    val decimal64Array: Array[java.lang.Long] = Array(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L,
      17L, 18L, 19L, 20L)
    val decimal128Array: Array[Array[Long]] = Array(
      Array(0L, 0L), Array(1L, 1L), Array(2L, 2L), Array(3L, 3L), Array(4L, 4L), Array(5L, 5L), Array(6L, 6L),
      Array(7L, 7L), Array(8L, 8L), Array(9L, 9L), Array(10L, 10L), Array(11L, 11L), Array(12L, 12L), Array(13L, 13L),
      Array(14L, 14L), Array(15L, 15L), Array(16L, 16L), Array(17L, 17L), Array(18L, 18L), Array(19L, 19L), Array(20L, 20L))
    val stringArray: Array[java.lang.String] = Array("", "a", "bb", "ccc", "dddd", "eeeee", "ffffff", "ggggggg",
      "hhhhhhhh", "iiiiiiiii", "jjjjjjjjjj", "kkkkkkkkkkk", "llllllllllll", "mmmmmmmmmmmmm", "nnnnnnnnnnnnnn",
      "ooooooooooooooo", "pppppppppppppppp", "qqqqqqqqqqqqqqqqq", "rrrrrrrrrrrrrrrrrr", "sssssssssssssssssss",
      "tttttttttttttttttttt")
    val booleanArray: Array[java.lang.Boolean] = Array(true, true, true, true, true, true, true, true, true, true,
      false, false, false, false, false, false, false, false, false, false, false)

    val pidVector0 = ColumnarShuffleWriterSuite.initOmniColumnIntVector(pidArray)
    val intVector0 = ColumnarShuffleWriterSuite.initOmniColumnIntVector(intArray)
    val shortVector0 = ColumnarShuffleWriterSuite.initOmniColumnShortVector(shortArray)
    val longVector0 = ColumnarShuffleWriterSuite.initOmniColumnLongVector(longArray)
    val doubleVector0 = ColumnarShuffleWriterSuite.initOmniColumnDoubleVector(doubleArray)
    val decimal64Vector0 = ColumnarShuffleWriterSuite.initOmniColumnDecimal64Vector(decimal64Array)
    val decimal128Vector0 = ColumnarShuffleWriterSuite.initOmniColumnDecimal128Vector(decimal128Array)
    val varcharVector0 = ColumnarShuffleWriterSuite.initOmniColumnVarcharVector(stringArray)
    val booleanVector0 = ColumnarShuffleWriterSuite.initOmniColumnBooleanVector(booleanArray)

    val cb0 = ColumnarShuffleWriterSuite.makeColumnarBatch(
      pidVector0.getVec.getSize,
      List(pidVector0, intVector0, shortVector0, longVector0, doubleVector0,
        decimal64Vector0, decimal128Vector0, varcharVector0, booleanVector0)
    )

    val pidVector1 = ColumnarShuffleWriterSuite.initOmniColumnIntVector(pidArray)
    val intVector1 = ColumnarShuffleWriterSuite.initOmniColumnIntVector(intArray)
    val shortVector1 = ColumnarShuffleWriterSuite.initOmniColumnShortVector(shortArray)
    val longVector1 = ColumnarShuffleWriterSuite.initOmniColumnLongVector(longArray)
    val doubleVector1 = ColumnarShuffleWriterSuite.initOmniColumnDoubleVector(doubleArray)
    val decimal64Vector1 = ColumnarShuffleWriterSuite.initOmniColumnDecimal64Vector(decimal64Array)
    val decimal128Vector1 = ColumnarShuffleWriterSuite.initOmniColumnDecimal128Vector(decimal128Array)
    val varcharVector1 = ColumnarShuffleWriterSuite.initOmniColumnVarcharVector(stringArray)
    val booleanVector1 = ColumnarShuffleWriterSuite.initOmniColumnBooleanVector(booleanArray)

    val cb1 = ColumnarShuffleWriterSuite.makeColumnarBatch(
      pidVector1.getVec.getSize,
      List(pidVector1, intVector1, shortVector1, longVector1, doubleVector1,
        decimal64Vector1, decimal128Vector1, varcharVector1, booleanVector1)
    )

    def records: Iterator[(Int, ColumnarBatch)] = Iterator((0, cb0), (0, cb1))

    val writer = new ColumnarShuffleWriter[Int, ColumnarBatch](
      blockResolver,
      shuffleHandle,
      0L, // MapId
      taskContext.taskMetrics().shuffleWriteMetrics)

    writer.write(records)
    writer.stop(success = true)

    assert(writer.getPartitionLengths.sum === outputFile.length())
    assert(writer.getPartitionLengths.count(_ == 0L) === 0)
    // should be (numPartitions - 2) zero length files

    val shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics
    assert(shuffleWriteMetrics.bytesWritten === outputFile.length())
    assert(shuffleWriteMetrics.recordsWritten === records.length)

    assert(taskMetrics.diskBytesSpilled === 0)
    assert(taskMetrics.memoryBytesSpilled === 0)

    val serializer = new ColumnarBatchSerializer(avgBatchNumRows, outputNumRows).newInstance()
    val deserializedStream = serializer.deserializeStream(new FileInputStream(outputFile))

    try {
      val kv = deserializedStream.asKeyValueIterator
      var length = 0
      kv.foreach {
        case (_, batch: ColumnarBatch) =>
          length += 1
          assert(batch.numRows == 42)
          assert(batch.numCols == 8)
          assert(batch.column(0).asInstanceOf[OmniColumnVector].getVec.asInstanceOf[IntVec].get(0) == 0)
          assert(batch.column(0).asInstanceOf[OmniColumnVector].getVec.asInstanceOf[IntVec].get(19) == 19)
          assert(batch.column(1).asInstanceOf[OmniColumnVector].getVec.asInstanceOf[ShortVec].get(0) == 0)
          assert(batch.column(1).asInstanceOf[OmniColumnVector].getVec.asInstanceOf[ShortVec].get(19) == 19)
          assert(batch.column(2).asInstanceOf[OmniColumnVector].getVec.asInstanceOf[LongVec].get(0) == 0)
          assert(batch.column(2).asInstanceOf[OmniColumnVector].getVec.asInstanceOf[LongVec].get(19) == 19)
          assert(batch.column(3).asInstanceOf[OmniColumnVector].getVec.asInstanceOf[DoubleVec].get(0) == 0.0)
          assert(batch.column(3).asInstanceOf[OmniColumnVector].getVec.asInstanceOf[DoubleVec].get(19) == 19.19)
          assert(batch.column(4).asInstanceOf[OmniColumnVector].getVec.asInstanceOf[LongVec].get(0) == 0L)
          assert(batch.column(4).asInstanceOf[OmniColumnVector].getVec.asInstanceOf[LongVec].get(19) == 19L)
          assert(batch.column(5).asInstanceOf[OmniColumnVector].getVec.asInstanceOf[Decimal128Vec].get(0) sameElements Array(0L, 0L))
          assert(batch.column(5).asInstanceOf[OmniColumnVector].getVec.asInstanceOf[Decimal128Vec].get(19) sameElements Array(19L, 19L))
          assert(batch.column(6).asInstanceOf[OmniColumnVector].getVec.asInstanceOf[VarcharVec].get(0) sameElements "")
          assert(batch.column(6).asInstanceOf[OmniColumnVector].getVec.asInstanceOf[VarcharVec].get(19) sameElements "sssssssssssssssssss")
          assert(batch.column(7).asInstanceOf[OmniColumnVector].getVec.asInstanceOf[BooleanVec].get(0) == true)
          assert(batch.column(7).asInstanceOf[OmniColumnVector].getVec.asInstanceOf[BooleanVec].get(19) == false)
          (0 until batch.numCols).foreach { i =>
            val valueVector = batch.column(i).asInstanceOf[OmniColumnVector].getVec
            assert(valueVector.getSize == batch.numRows)
          }
          batch.close()
      }
      assert(length == 1)
    } finally {
      deserializedStream.close()
    }

  }
}
