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
import nova.hetu.omniruntime.`type`.Decimal64DataType
import nova.hetu.omniruntime.vector._
import org.apache.spark.{HashPartitioner, SparkConf, TaskContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.sort.ColumnarShuffleHandle
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.util.Utils
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Mockito.{doAnswer, when}
import org.mockito.invocation.InvocationOnMock

class ColumnarShuffleWriterSuite extends SharedSparkSession {
  @Mock(answer = RETURNS_SMART_NULLS) private var taskContext: TaskContext = _
  @Mock(answer = RETURNS_SMART_NULLS) private var blockResolver: IndexShuffleBlockResolver = _
  @Mock(answer = RETURNS_SMART_NULLS) private var dependency
  : ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = _

  override def sparkConf: SparkConf =
    super.sparkConf
      .setAppName("test ColumnarShuffleWriter")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")

  private var taskMetrics: TaskMetrics = _
  private var tempDir: File = _
  private var outputFile: File = _

  private var shuffleHandle: ColumnarShuffleHandle[Int, ColumnarBatch] = _
  private val numPartitions = 11

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

    val inputTypes = "[{\"id\":1}," +
                      "{\"id\":1}," +
                      "{\"id\":6,\"precision\":18,\"scale\":3}," +
                      "{\"id\":7,\"precision\":28,\"scale\":11}]"

    when(dependency.partitioner).thenReturn(new HashPartitioner(numPartitions))
    when(dependency.serializer).thenReturn(new JavaSerializer(sparkConf))
    when(dependency.partitionInfo).thenReturn(
      new PartitionInfo("hash", numPartitions, 4, inputTypes))
    // inputTypes e.g:
    // [{"id":"OMNI_INT","width":0,"precision":0,"scale":0,"dateUnit":"DAY","timeUnit":"SEC"},
    // {"id":"OMNI_INT","width":0,"precision":0,"scale":0,"dateUnit":"DAY","timeUnit":"SEC"}]
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

  test("write empty iterator") {
    val writer = new ColumnarShuffleWriter[Int, ColumnarBatch](
      blockResolver,
      shuffleHandle,
      0, // MapId
      taskContext.taskMetrics().shuffleWriteMetrics)
    writer.write(Iterator.empty)
    writer.stop( /* success = */ true)

    assert(writer.getPartitionLengths.sum === 0)
    assert(outputFile.exists())
    assert(outputFile.length() === 0)
    val shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics
    assert(shuffleWriteMetrics.bytesWritten === 0)
    assert(shuffleWriteMetrics.recordsWritten === 0)
    assert(taskMetrics.diskBytesSpilled === 0)
    assert(taskMetrics.memoryBytesSpilled === 0)
  }

  test("write empty column batch") {
    val vectorPid0 = ColumnarShuffleWriterSuite.initOmniColumnIntVector()
    val vector0_1 = ColumnarShuffleWriterSuite.initOmniColumnIntVector()
    val vector0_2 = ColumnarShuffleWriterSuite.initOmniColumnIntVector()
    val vector0_3 = ColumnarShuffleWriterSuite.initOmniColumnDecimal64Vector()
    val vector0_4 = ColumnarShuffleWriterSuite.initOmniColumnDecimal128Vector()

    val vectorPid1 = ColumnarShuffleWriterSuite.initOmniColumnIntVector()
    val vector1_1 = ColumnarShuffleWriterSuite.initOmniColumnIntVector()
    val vector1_2 = ColumnarShuffleWriterSuite.initOmniColumnIntVector()
    val vector1_3 = ColumnarShuffleWriterSuite.initOmniColumnDecimal64Vector()
    val vector1_4 = ColumnarShuffleWriterSuite.initOmniColumnDecimal128Vector()

    val cb0 = ColumnarShuffleWriterSuite.makeColumnarBatch(
      vectorPid0.getVec.getSize,List(vectorPid0, vector0_1, vector0_2, vector0_3, vector0_4))
    val cb1 = ColumnarShuffleWriterSuite.makeColumnarBatch(
      vectorPid1.getVec.getSize,List(vectorPid1, vector1_1, vector1_2, vector1_3, vector1_4))

    def records: Iterator[(Int, ColumnarBatch)] = Iterator((0, cb0), (0, cb1))

    val writer = new ColumnarShuffleWriter[Int, ColumnarBatch](
      blockResolver,
      shuffleHandle,
      0L, // MapId
      taskContext.taskMetrics().shuffleWriteMetrics)

    writer.write(records)
    writer.stop(success = true)
    assert(writer.getPartitionLengths.sum === 0)
    assert(outputFile.exists())
    assert(outputFile.length() === 0)
    val shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics
    assert(shuffleWriteMetrics.bytesWritten === 0)
    assert(shuffleWriteMetrics.recordsWritten === 0)
    assert(taskMetrics.diskBytesSpilled === 0)
    assert(taskMetrics.memoryBytesSpilled === 0)
  }

  test("write with some empty partitions") {
    val vectorPid0 = ColumnarShuffleWriterSuite.initOmniColumnIntVector(0, 0, 1, 1)
    val vector0_1 = ColumnarShuffleWriterSuite.initOmniColumnIntVector(null, null, null, null)
    val vector0_2 = ColumnarShuffleWriterSuite.initOmniColumnIntVector(100, 100, null, null)
    val vector0_3 = ColumnarShuffleWriterSuite.initOmniColumnDecimal64Vector(100L, 100L, 100L, 100L)
    val vector0_4 = ColumnarShuffleWriterSuite.initOmniColumnDecimal128Vector(Array(100L, 100L), Array(100L, 100L), null, null)
    val cb0 = ColumnarShuffleWriterSuite.makeColumnarBatch(
      vectorPid0.getVec.getSize,List(vectorPid0, vector0_1, vector0_2, vector0_3, vector0_4))

    val vectorPid1 = ColumnarShuffleWriterSuite.initOmniColumnIntVector(0, 0, 1, 1)
    val vector1_1 = ColumnarShuffleWriterSuite.initOmniColumnIntVector(null, null, null, null)
    val vector1_2 = ColumnarShuffleWriterSuite.initOmniColumnIntVector(100, 100, null, null)
    val vector1_3 = ColumnarShuffleWriterSuite.initOmniColumnDecimal64Vector(100L, 100L, 100L, 100L)
    val vector1_4 = ColumnarShuffleWriterSuite.initOmniColumnDecimal128Vector(Array(100L, 100L), Array(100L, 100L), null, null)
    val cb1 = ColumnarShuffleWriterSuite.makeColumnarBatch(
      vectorPid1.getVec.getSize,List(vectorPid1, vector1_1, vector1_2, vector1_3, vector1_4))

    def records: Iterator[(Int, ColumnarBatch)] = Iterator((0, cb0), (0, cb1))

    val writer = new ColumnarShuffleWriter[Int, ColumnarBatch](
      blockResolver,
      shuffleHandle,
      0L, // MapId
      taskContext.taskMetrics().shuffleWriteMetrics)

    writer.write(records)
    writer.stop(success = true)

    assert(writer.getPartitionLengths.sum === outputFile.length())
    assert(writer.getPartitionLengths.count(_ == 0L) === (numPartitions - 2))
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
          assert(batch.numRows == 4)
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
      assert(length == 2)
    } finally {
      deserializedStream.close()
    }

  }
}

object ColumnarShuffleWriterSuite {
  def initOmniColumnIntVector(values: Integer*): OmniColumnVector = {
    val length = values.length
    val vecTmp = new IntVec(length)
    (0 until length).foreach { i =>
      if (values(i) != null) {
        vecTmp.set(i, values(i).asInstanceOf[Int])
      }
    }
    val colVecTmp = new OmniColumnVector(length, IntegerType, false)
    colVecTmp.setVec(vecTmp)
    colVecTmp
  }

  def initOmniColumnDecimal64Vector(values: java.lang.Long*): OmniColumnVector = {
    val length = values.length
    val vecTmp = new LongVec(length)
    (0 until length).foreach { i =>
      if (values(i) != null) {
        vecTmp.set(i, values(i).asInstanceOf[Long])
      }
    }
    val colVecTmp = new OmniColumnVector(length, DecimalType(18, 3), false)
    colVecTmp.setVec(vecTmp)
    colVecTmp
  }

  def initOmniColumnDecimal128Vector(values: Array[Long]*): OmniColumnVector = {
    val length = values.length
    val vecTmp = new Decimal128Vec(length)
    (0 until length).foreach { i =>
      if (values(i) != null) {
        vecTmp.set(i, values(i))
      }
    }
    val colVecTmp = new OmniColumnVector(length, DecimalType(28, 11), false)
    colVecTmp.setVec(vecTmp)
    colVecTmp
  }

  def makeColumnarBatch(rowNum: Int, vectors: List[ColumnVector]): ColumnarBatch = {
    new ColumnarBatch(vectors.toArray, rowNum)
  }
}
