/*
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

package org.apache.spark.sql.execution.util

import com.huawei.boostkit.spark.ColumnarPluginConfig

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor.sparkTypeToOmniType
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.transColBatchToOmniVecs
import nova.hetu.omniruntime.`type`.{DataType, VarcharDataType}
import nova.hetu.omniruntime.vector.{BooleanVec, Decimal128Vec, DoubleVec, IntVec, LongVec, ShortVec, VarcharVec, Vec, VecBatch}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.types.{BooleanType, DateType, DecimalType, DoubleType, IntegerType, LongType, ShortType, StringType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class MergeIterator(iter: Iterator[ColumnarBatch], localSchema: StructType,
                    numMergedVecBatchs: SQLMetric) extends Iterator[ColumnarBatch] {

  private val outputQueue = new mutable.Queue[VecBatch]
  private val bufferedVecBatch = new ListBuffer[VecBatch]()
  val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf
  private val maxBatchSizeInBytes: Int = columnarConf.maxBatchSizeInBytes
  private val maxRowCount: Int = columnarConf.maxRowCount
  private var totalRows = 0
  private var currentBatchSizeInBytes = 0

  private def createOmniVectors(schema: StructType, columnSize: Int): Array[Vec] = {
    val vecs = new Array[Vec](schema.fields.length)
    schema.fields.zipWithIndex.foreach { case (field, index) =>
      field.dataType match {
        case LongType =>
          vecs(index) = new LongVec(columnSize)
        case DateType | IntegerType =>
          vecs(index) = new IntVec(columnSize)
        case ShortType =>
          vecs(index) = new ShortVec(columnSize)
        case DoubleType =>
          vecs(index) = new DoubleVec(columnSize)
        case BooleanType =>
          vecs(index) = new BooleanVec(columnSize)
        case StringType =>
          val vecType: DataType = sparkTypeToOmniType(field.dataType, field.metadata)
          vecs(index) = new VarcharVec(vecType.asInstanceOf[VarcharDataType].getWidth * columnSize,
            columnSize)
        case dt: DecimalType =>
          if (DecimalType.is64BitDecimalType(dt)) {
            vecs(index) = new LongVec(columnSize)
          } else {
            vecs(index) = new Decimal128Vec(columnSize)
          }
        case _ =>
          throw new UnsupportedOperationException("Fail to create omni vector, unsupported fields")
      }
    }
    vecs
  }

  private def buffer(vecBatch: VecBatch): Unit = {
    var totalSize = 0
    vecBatch.getVectors.zipWithIndex.foreach {
      case (vec, i) =>
        totalSize += vec.getCapacityInBytes
    }
    currentBatchSizeInBytes += totalSize
    totalRows += vecBatch.getRowCount

    bufferedVecBatch.append(vecBatch)
    if (isFull()) {
      flush()
    }
  }

  private def merge(resultBatch: VecBatch, bufferedBatch: ListBuffer[VecBatch]): Unit = {
    localSchema.fields.zipWithIndex.foreach { case (field, index) =>
      var offset = 0
      for (elem <- bufferedBatch) {
        val src: Vec = elem.getVector(index)
        val dest: Vec = resultBatch.getVector(index)
        dest.append(src, offset, elem.getRowCount)
        offset += elem.getRowCount
        src.close()
      }
    }
  }

  private def flush(): Unit = {

    if (bufferedVecBatch.isEmpty) {
      return
    }
    val resultBatch: VecBatch = new VecBatch(createOmniVectors(localSchema, totalRows), totalRows)
    merge(resultBatch, bufferedVecBatch)
    outputQueue.enqueue(resultBatch)
    numMergedVecBatchs += 1

    bufferedVecBatch.clear()
    currentBatchSizeInBytes = 0
    totalRows = 0

  }

  private def vecBatchToColumnarBatch(vecBatch: VecBatch): ColumnarBatch = {
    val vectors: Seq[OmniColumnVector] = OmniColumnVector.allocateColumns(
      vecBatch.getRowCount, localSchema, false)
    vectors.zipWithIndex.foreach { case (vector, i) =>
        vector.reset()
        vector.setVec(vecBatch.getVectors()(i))
    }
    vecBatch.close()
    new ColumnarBatch(vectors.toArray, vecBatch.getRowCount)
  }

  override def hasNext: Boolean = {
    while (outputQueue.isEmpty && iter.hasNext) {
      val batch: ColumnarBatch = iter.next()
      val input: Array[Vec] = transColBatchToOmniVecs(batch)
      val vecBatch = new VecBatch(input, batch.numRows())
      buffer(vecBatch)
    }

    if (outputQueue.isEmpty && bufferedVecBatch.isEmpty) {
      false
    } else {
      true
    }
  }

  override def next(): ColumnarBatch = {
    if (outputQueue.nonEmpty) {
      vecBatchToColumnarBatch(outputQueue.dequeue())
    } else if (bufferedVecBatch.nonEmpty) {
      flush()
      vecBatchToColumnarBatch(outputQueue.dequeue())
    } else {
      throw new RuntimeException("bufferedVecBatch and outputQueue are empty")
    }
  }


  def isFull(): Boolean = {
    totalRows > maxRowCount || currentBatchSizeInBytes >= maxBatchSizeInBytes
  }
}
