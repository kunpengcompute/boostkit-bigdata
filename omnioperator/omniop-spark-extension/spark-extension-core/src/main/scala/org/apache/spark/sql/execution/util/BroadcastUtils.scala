/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

import com.huawei.boostkit.spark.util.OmniAdaptorUtil.transColBatchToOmniVecs
import org.apache.spark.{SparkConf, SparkContext, TaskContext, TaskContextImpl}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{TaskMemoryManager, UnifiedMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, BroadcastPartitioning, IdentityBroadcastMode, Partitioning}
import org.apache.spark.sql.execution.ColumnarHashedRelation
import org.apache.spark.sql.execution.joins.{HashedRelation, HashedRelationBroadcastMode}
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.Properties
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import nova.hetu.omniruntime.vector.serialize.VecBatchSerializerFactory
import nova.hetu.omniruntime.vector.VecBatch
import org.apache.spark.sql.util.ShimUtil

object BroadcastUtils {

  def getBroadCastMode(partitioning: Partitioning): BroadcastMode = {
    partitioning match {
      case BroadcastPartitioning(mode) =>
        mode
      case _ =>
        throw new IllegalArgumentException("Unexpected partitioning: " + partitioning.toString)
    }
  }

  def omniToSparkUnsafe[F, T](
                               context: SparkContext,
                               mode: BroadcastMode,
                               from: Broadcast[F],
                               fromSchema: StructType,
                               fn: Iterator[ColumnarBatch] => Iterator[InternalRow]): Broadcast[T] = {
    mode match {
      case HashedRelationBroadcastMode(_, _) =>
        // ColumnarHashedRelation to HashedRelation.
        val fromBroadcast = from.asInstanceOf[Broadcast[ColumnarHashedRelation]]
        val fromRelation = fromBroadcast.value
        val toRelation = runUnSafe(context.getConf) {
          val (rowCount, rowIterator) = deserializeRelation(fromSchema, fn, fromRelation)
          mode.transform(rowIterator, Some(rowCount))
        }
        // Rebroadcast Spark relation.
        context.broadcast(toRelation).asInstanceOf[Broadcast[T]]
      case IdentityBroadcastMode =>
        // ColumnarBuildSideRelation to Array.
        val fromBroadcast = from.asInstanceOf[Broadcast[ColumnarHashedRelation]]
        val fromRelation = fromBroadcast.value
        val toRelation = runUnSafe(context.getConf) {
          val (_, rowIterator) = deserializeRelation(fromSchema, fn, fromRelation)
          val rowArray = new ArrayBuffer[InternalRow]()
          while (rowIterator.hasNext) {
            val unsafeRow = rowIterator.next().asInstanceOf[UnsafeRow]
            rowArray.append(unsafeRow.copy())
          }
          rowArray.toArray
        }
        context.broadcast(toRelation).asInstanceOf[Broadcast[T]]
      case _ => throw new IllegalStateException("Unexpected broadcast mode: " + mode)
    }
  }

  def sparkToOmniUnsafe[F, T](context: SparkContext,
                              mode: BroadcastMode,
                              from: Broadcast[F],
                              logFunc: (=> String) => Unit,
                              fn: Iterator[InternalRow] => Iterator[ColumnarBatch]): Broadcast[T] = {
    mode match {
      case HashedRelationBroadcastMode(_, _) =>
        //  HashedRelation to ColumnarHashedRelation
        val fromBroadcast = from.asInstanceOf[Broadcast[HashedRelation]]
        val fromRelation = fromBroadcast.value
        val toRelation = runUnSafe(context.getConf) {
          val (nullBatchCount, input) = serializeRelation(mode, fn(fromRelation.keys().flatMap(fromRelation.get)), logError = logFunc)
          val relation = new ColumnarHashedRelation
          relation.converterData(mode, nullBatchCount, input)
          relation
        }
        // Rebroadcast Omni relation.
        context.broadcast(toRelation).asInstanceOf[Broadcast[T]]
      case IdentityBroadcastMode =>
        // ColumnarBuildSideRelation to Array.
        val fromBroadcast = from.asInstanceOf[Broadcast[Array[InternalRow]]]
        val fromRelation = fromBroadcast.value
        val toRelation = runUnSafe(context.getConf) {
          val (nullBatchCount, input) = serializeRelation(mode, fn(fromRelation.toIterator), logError = logFunc)
          val relation = new ColumnarHashedRelation
          relation.converterData(mode, nullBatchCount, input)
          relation
        }
        // Rebroadcast Omni relation.
        context.broadcast(toRelation).asInstanceOf[Broadcast[T]]
      case _ => throw new IllegalStateException("Unexpected broadcast mode: " + mode)
    }
  }

  private def deserializeRelation(fromSchema: StructType,
                                  fn: Iterator[ColumnarBatch] => Iterator[InternalRow],
                                  fromRelation: ColumnarHashedRelation): (Long, Iterator[InternalRow]) = {
    val deserializer = VecBatchSerializerFactory.create()
    val data = fromRelation.buildData
    var rowCount = 0
    val batchBuffer = ListBuffer[ColumnarBatch]()
    val rowIterator = fn(
      try {
        data.map(bytes => {
          val batch: VecBatch = deserializer.deserialize(bytes)
          val columnarBatch = vecBatchToColumnarBatch(batch, fromSchema)
          batchBuffer.append(columnarBatch)
          rowCount += columnarBatch.numRows()
          columnarBatch
        }).toIterator
      } catch {
        case exception: Exception =>
          batchBuffer.foreach(_.close())
          throw exception
      }
    )
    (rowCount, rowIterator)
  }

  private def serializeRelation(mode: BroadcastMode,
                                batches: Iterator[ColumnarBatch],
                                logError: (=> String) => Unit): (Int, Array[Array[Byte]]) = {
    val serializer = VecBatchSerializerFactory.create()
    var nullBatchCount = 0
    val nullRelationFlag = mode match {
      case hashRelMode: HashedRelationBroadcastMode =>
        hashRelMode.isNullAware
      case _ => false
    }
    val input: Array[Array[Byte]] = batches.map(batch => {
      // When nullRelationFlag is true, it means anti-join
      // Only one column of data is involved in the anti-
      if (nullRelationFlag && batch.numCols() > 0) {
        val vec = batch.column(0)
        if (vec.hasNull) {
          try {
            nullBatchCount += 1
          } catch {
            case e: Exception =>
              logError(s"compute null BatchCount error : ${e.getMessage}.")
          }
        }
      }
      val vectors = transColBatchToOmniVecs(batch)
      val vecBatch = new VecBatch(vectors, batch.numRows())
      val vecBatchSer = serializer.serialize(vecBatch)
      // close omni vec
      vecBatch.releaseAllVectors()
      vecBatch.close()
      vecBatchSer
    }).toArray
    (nullBatchCount, input)
  }

  private def vecBatchToColumnarBatch(vecBatch: VecBatch, schema: StructType): ColumnarBatch = {
    val vectors: Seq[OmniColumnVector] = OmniColumnVector.allocateColumns(
      vecBatch.getRowCount, schema, false)
    vectors.zipWithIndex.foreach { case (vector, i) =>
      vector.reset()
      vector.setVec(vecBatch.getVectors()(i))
    }
    vecBatch.close()
    new ColumnarBatch(vectors.toArray, vecBatch.getRowCount)
  }

  // Run code with unsafe task context. If the call took place from Spark driver or test code
  // without a Spark task context registered, a temporary unsafe task context instance will
  // be created and used. Since unsafe task context is not managed by Spark's task memory manager,
  // Spark may not be aware of the allocations happened inside the user code.
  //
  // The API should only be used in the following cases:
  //
  // 1. Run code on driver
  // 2. Run test code
  private def runUnSafe[T](sparkConf: SparkConf)(body: => T): T = {
    if (inSparkTask()) {
      throw new UnsupportedOperationException("runUnsafe should only be used outside Spark task")
    }
    TaskContext.setTaskContext(createUnSafeTaskContext(sparkConf))
    val context = getLocalTaskContext()
    try {
      val out =
        try {
          body
        } catch {
          case t: Throwable =>
            // Similar code with those in Task.scala
            try {
              context.markTaskFailed(t)
            } catch {
              case t: Throwable =>
                t.addSuppressed(t)
            }
            context.markTaskCompleted(Some(t))
            throw t
        } finally {
          try {
            context.markTaskCompleted(None)
          } finally {
            unsetUnsafeTaskContext()
          }
        }
      out
    } catch {
      case t: Throwable =>
        throw t
    }
  }

  private def getLocalTaskContext(): TaskContext = {
    TaskContext.get()
  }

  private def inSparkTask(): Boolean = {
    TaskContext.get() != null
  }

  private def unsetUnsafeTaskContext(): Unit = {
    if (!inSparkTask()) {
      throw new IllegalStateException()
    }
    if (getLocalTaskContext().taskAttemptId() != -1) {
      throw new IllegalStateException()
    }
    TaskContext.unset()
  }

  private def createUnSafeTaskContext(sparkConf: SparkConf): TaskContext = {
    // driver code run on unsafe task context which is not managed by Spark's task memory manager,
    // so the maxHeapMemory set  ByteUnit.TiB.toBytes(2)
    val memoryManager =
      new UnifiedMemoryManager(sparkConf, ByteUnit.TiB.toBytes(2), ByteUnit.TiB.toBytes(1), 1)
    ShimUtil.createTaskContext(
      -1,
      -1,
      -1,
      -1L,
      -1,
      -1,
      new TaskMemoryManager(memoryManager, -1L),
      new Properties,
      MetricsSystem.createMetricsSystem("OMNI_UNSAFE", sparkConf),
      TaskMetrics.empty,
      1,
      Map.empty
    )
  }

}
