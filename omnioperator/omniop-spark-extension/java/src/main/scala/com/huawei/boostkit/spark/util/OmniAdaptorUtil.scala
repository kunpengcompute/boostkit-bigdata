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

package com.huawei.boostkit.spark.util

import com.huawei.boostkit.spark.Constant.IS_SKIP_VERIFY_EXP

import java.util.concurrent.TimeUnit.NANOSECONDS
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor._
import nova.hetu.omniruntime.constants.FunctionType
import nova.hetu.omniruntime.operator.OmniOperator
import nova.hetu.omniruntime.operator.aggregator.{OmniAggregationWithExprOperatorFactory, OmniHashAggregationWithExprOperatorFactory}
import nova.hetu.omniruntime.operator.config.{OperatorConfig, OverflowConfig, SpillConfig}
import nova.hetu.omniruntime.vector._
import org.apache.spark.sql.catalyst.expressions.{Attribute, ExprId, NamedExpression, SortOrder}
import org.apache.spark.sql.execution.datasources.orc.OrcColumnVector
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.vectorized.{OmniColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import java.util

object OmniAdaptorUtil {
  def transColBatchToOmniVecs(cb: ColumnarBatch): Array[Vec] = {
    transColBatchToOmniVecs(cb, false)
  }

  def transColBatchToOmniVecs(cb: ColumnarBatch, isSlice: Boolean): Array[Vec] = {
    val input = new Array[Vec](cb.numCols())
    for (i <- 0 until cb.numCols()) {
      val omniVec: Vec = cb.column(i) match {
        case vector: OrcColumnVector =>
          transColumnVector(vector, cb.numRows())
        case vector: OnHeapColumnVector =>
          transColumnVector(vector, cb.numRows())
        case vector: OmniColumnVector =>
          if (!isSlice) {
            vector.getVec
          } else {
            vector.getVec.slice(0, cb.numRows())
          }
        case _ =>
          throw new UnsupportedOperationException("unsupport column vector!")
      }
      input(i) = omniVec
    }
    input
  }

  def transColumnVector(columnVector: ColumnVector, columnSize : Int): Vec = {
    val datatype: DataType = columnVector.dataType()
    val vec: Vec = datatype match {
      case LongType =>
        val vec = new LongVec(columnSize)
        val values = new Array[Long](columnSize)
        for (i <- 0 until columnSize) {
          if (!columnVector.isNullAt(i)) {
            values(i) = columnVector.getLong(i)
          } else {
            vec.setNull(i)
          }
        }
        vec.put(values, 0, 0, columnSize)
        vec
      case DateType | IntegerType =>
        val vec = new IntVec(columnSize)
        val values = new Array[Int](columnSize)
        for (i <- 0 until columnSize) {
          if (!columnVector.isNullAt(i)) {
            values(i) = columnVector.getInt(i)
          } else {
            vec.setNull(i)
          }
        }
        vec.put(values, 0, 0, columnSize)
        vec
      case ShortType =>
        val vec = new ShortVec(columnSize)
        val values = new Array[Short](columnSize)
        for (i <- 0 until columnSize) {
          if (!columnVector.isNullAt(i)) {
            values(i) = columnVector.getShort(i)
          } else {
            vec.setNull(i)
          }
        }
        vec.put(values, 0, 0, columnSize)
        vec
      case DoubleType =>
        val vec = new DoubleVec(columnSize)
        val values = new Array[Double](columnSize)
        for (i <- 0 until columnSize) {
          if (!columnVector.isNullAt(i)) {
            values(i) = columnVector.getDouble(i)
          } else {
            vec.setNull(i)
          }
        }
        vec.put(values, 0, 0, columnSize)
        vec
      case StringType =>
        var totalSize = 0
        val offsets = new Array[Int](columnSize + 1)
        for (i <- 0 until columnSize) {
          if (null != columnVector.getUTF8String(i)) {
            val strLen: Int = columnVector.getUTF8String(i).getBytes.length
            totalSize += strLen
          }
          offsets(i + 1) = totalSize
        }
        val vec = new VarcharVec(totalSize, columnSize)
        val values = new Array[Byte](totalSize)
        for (i <- 0 until columnSize) {
          if (null != columnVector.getUTF8String(i)) {
            System.arraycopy(columnVector.getUTF8String(i).getBytes, 0, values,
              offsets(i), offsets(i + 1) - offsets(i))
          } else {
            vec.setNull(i)
          }
        }
        vec.put(0, values, 0, offsets, 0, columnSize)
        vec
      case BooleanType =>
        val vec = new BooleanVec(columnSize)
        val values = new Array[Boolean](columnSize)
        for (i <- 0 until columnSize) {
          if (!columnVector.isNullAt(i)) {
            values(i) = columnVector.getBoolean(i)
          } else {
            vec.setNull(i)
          }
        }
        vec.put(values, 0, 0, columnSize)
        vec
      case t: DecimalType =>
        if (DecimalType.is64BitDecimalType(datatype)) {
          val vec = new LongVec(columnSize)
          val values = new Array[Long](columnSize)
          for (i <- 0 until columnSize) {
            if (!columnVector.isNullAt(i)) {
              values(i) = columnVector.getDecimal(i, t.precision, t.scale).toUnscaledLong
            } else {
              vec.setNull(i)
            }
          }
          vec.put(values, 0, 0, columnSize)
          vec
        } else {
          val vec = new Decimal128Vec(columnSize)
          for (i <- 0 until columnSize) {
            if (!columnVector.isNullAt(i)) {
              vec.setBigInteger(i,
                columnVector.getDecimal(i, t.precision, t.scale).toJavaBigDecimal.unscaledValue())
            } else {
              vec.setNull(i)
            }
          }
          vec
        }
      case _ =>
        throw new UnsupportedOperationException("unsupport column vector!")
    }
    vec
  }

  def genSortParam(output: Seq[Attribute], sortOrder: Seq[SortOrder]):
      (Array[nova.hetu.omniruntime.`type`.DataType], Array[Int], Array[Int], Array[String]) = {
    val inputColSize: Int = output.size
    val sourceTypes = new Array[nova.hetu.omniruntime.`type`.DataType](inputColSize)
    val ascendings = new Array[Int](sortOrder.size)
    val nullFirsts = new Array[Int](sortOrder.size)
    val sortColsExp = new Array[String](sortOrder.size)
    val omniAttrExpsIdMap: Map[ExprId, Int] = getExprIdMap(output)

    output.zipWithIndex.foreach { case (inputAttr, i) =>
      sourceTypes(i) = sparkTypeToOmniType(inputAttr.dataType, inputAttr.metadata)
    }
    sortOrder.zipWithIndex.foreach { case (sortAttr, i) =>
      sortColsExp(i) = rewriteToOmniJsonExpressionLiteral(sortAttr.child, omniAttrExpsIdMap)
      ascendings(i) = if (sortAttr.isAscending) {
        1
      } else {
        0
      }
      nullFirsts(i) = sortAttr.nullOrdering.sql match {
        case "NULLS LAST" => 0
        case _ => 1
      }
    }
    if (!isSimpleColumnForAll(sortColsExp)) {
      checkOmniJsonWhiteList("", sortColsExp.asInstanceOf[Array[AnyRef]])
    }
    (sourceTypes, ascendings, nullFirsts, sortColsExp)
  }

  def addAllAndGetIterator(operator: OmniOperator,
                           inputIter: Iterator[ColumnarBatch], schema: StructType,
                           addInputTime: SQLMetric, numInputVecBatchs: SQLMetric,
                           numInputRows: SQLMetric, getOutputTime: SQLMetric,
                           numOutputVecBatchs: SQLMetric, numOutputRows: SQLMetric,
                           outputDataSize: SQLMetric): Iterator[ColumnarBatch] = {
    while (inputIter.hasNext) {
      val batch: ColumnarBatch = inputIter.next()
      numInputVecBatchs += 1
      val input: Array[Vec] = transColBatchToOmniVecs(batch)
      val vecBatch = new VecBatch(input, batch.numRows())
      val startInput: Long = System.nanoTime()
      operator.addInput(vecBatch)
      addInputTime += NANOSECONDS.toMillis(System.nanoTime() - startInput)
      numInputRows += batch.numRows()
    }
    val startGetOp: Long = System.nanoTime()
    val results: util.Iterator[VecBatch] = operator.getOutput
    getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)

    new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = {
        val startGetOp: Long = System.nanoTime()
        val hasNext = results.hasNext
        getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)
        hasNext
      }

      override def next(): ColumnarBatch = {
        val startGetOp: Long = System.nanoTime()
        val vecBatch: VecBatch = results.next()
        getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)
        val vectors: Seq[OmniColumnVector] = OmniColumnVector.allocateColumns(
          vecBatch.getRowCount, schema, false)
        vectors.zipWithIndex.foreach { case (vector, i) =>
          vector.reset()
          vector.setVec(vecBatch.getVectors()(i))
          outputDataSize += vecBatch.getVectors()(i).getRealValueBufCapacityInBytes
          outputDataSize += vecBatch.getVectors()(i).getRealNullBufCapacityInBytes
          outputDataSize += vecBatch.getVectors()(i).getRealOffsetBufCapacityInBytes
        }
        val sourceLength = vecBatch.getVectorCount
        var destLength = schema.fields.length
        while (destLength < sourceLength) {
          vecBatch.getVectors()(destLength).close() // vecBatch releasing redundant columns
          destLength += 1
        }

        // metrics
        val rowCnt: Int = vecBatch.getRowCount
        numOutputRows += rowCnt
        numOutputVecBatchs += 1
        // close omni vecbetch
        vecBatch.close()
        new ColumnarBatch(vectors.toArray, rowCnt)
      }
    }
  }

  def overflowConf(): OverflowConfig.OverflowConfigId = {
    if (SQLConf.get.ansiEnabled)
      OverflowConfig.OverflowConfigId.OVERFLOW_CONFIG_EXCEPTION
    else
      OverflowConfig.OverflowConfigId.OVERFLOW_CONFIG_NULL
  }

  def getAggOperator(groupingExpressions: Seq[NamedExpression],
                     omniGroupByChanel: Array[String],
                     omniAggChannels: Array[Array[String]],
                     omniSourceTypes: Array[nova.hetu.omniruntime.`type`.DataType],
                     omniAggFunctionTypes: Array[FunctionType],
                     omniAggOutputTypes: Array[Array[nova.hetu.omniruntime.`type`.DataType]],
                     omniInputRaws: Array[Boolean],
                     omniOutputPartials: Array[Boolean]): OmniOperator = {
    var operator: OmniOperator = null
    if (groupingExpressions.nonEmpty) {
      operator = new OmniHashAggregationWithExprOperatorFactory(
        omniGroupByChanel,
        omniAggChannels,
        omniSourceTypes,
        omniAggFunctionTypes,
        omniAggOutputTypes,
        omniInputRaws,
        omniOutputPartials,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP)).createOperator
    } else {
      operator = new OmniAggregationWithExprOperatorFactory(
        omniGroupByChanel,
        omniAggChannels,
        omniSourceTypes,
        omniAggFunctionTypes,
        omniAggOutputTypes,
        omniInputRaws,
        omniOutputPartials,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP)).createOperator
    }
    operator
  }
}
