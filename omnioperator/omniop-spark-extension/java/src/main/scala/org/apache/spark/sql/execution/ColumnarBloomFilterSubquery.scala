/*
 * Copyright (C) 2023-2023. Huawei Technologies Co., Ltd. All rights reserved.
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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExprId, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import nova.hetu.omniruntime.operator.filter.OmniBloomFilterOperatorFactory
import nova.hetu.omniruntime.vector.{IntVec, LongVec, Vec, VecBatch}
import org.apache.spark.{ SparkContext, SparkEnv, SparkException}
import org.apache.spark.sql.execution.util.SparkMemoryUtils.addLeakSafeTaskCompletionListener

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, OutputStream}

case class ColumnarBloomFilterSubquery(plan: BaseSubqueryExec, exprId: ExprId, scalarSubquery: ScalarSubquery) extends ExecSubqueryExpression {
  // the first column in first row form `query`.
  @volatile private var result: Any = _
  protected var out: OutputStream = null
  private val writeBuffer = new Array[Byte](8)
  protected var writtenCount = 0
  protected var bloomFilterNativeAddress: Long = 0

  override def dataType: org.apache.spark.sql.types.DataType = scalarSubquery.dataType
  override def children: Seq[Expression] = Nil
  override def nullable: Boolean = true
  override def toString: String = scalarSubquery.toString
  override def eval(input: InternalRow): Any = {
    var ret = 0L
    // if eval at driver side, return 0
    if (SparkEnv.get.executorId != SparkContext.DRIVER_IDENTIFIER) {
      result = scalarSubquery.eval(input)
      if (result != null) {
        ret = copyToNativeBloomFilter()
      }
    }
    ret
  }
  override def withNewPlan(query: BaseSubqueryExec): ColumnarBloomFilterSubquery = copy(plan = scalarSubquery.plan)
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = scalarSubquery.doGenCode(ctx, ev)
  override def updateResult(): Unit = scalarSubquery.updateResult()

  def copyToNativeBloomFilter(): Long = {
    if (bloomFilterNativeAddress != 0L) {
      return bloomFilterNativeAddress
    }
    val vecBatch = convertByteArray2VecBatch(result.asInstanceOf[Array[Byte]])
    val bloomFilterOperatorFactory = new OmniBloomFilterOperatorFactory(1)
    val bloomFilterOperator = bloomFilterOperatorFactory.createOperator
    // close operator
    addLeakSafeTaskCompletionListener[Unit](_ => {
      bloomFilterOperator.close()
    })

    bloomFilterOperator.addInput(vecBatch)
    val outputs: java.util.Iterator[VecBatch] = bloomFilterOperator.getOutput

    // return BloomFilter off-heap address
    assert(outputs.hasNext, s"Expects bloom filter address value, but got nothing.")
    bloomFilterNativeAddress = outputs.next().getVector(0).asInstanceOf[LongVec].get(0)
    bloomFilterNativeAddress
  }

  def convertByteArray2VecBatch(buf: Array[Byte]): VecBatch = {
    val byteArrayLength = buf.length
    val intVecSize = byteArrayLength / java.lang.Integer.BYTES
    val checkVal = byteArrayLength % java.lang.Integer.BYTES
    if (checkVal != 0) {
      throw new SparkException(s"ColumnarBloomFilterSubquery result length is abnormal. ")
    }

    // deserialize
    val in = new ByteArrayInputStream(buf)
    val dis = new DataInputStream(in)
    val version = dis.readInt
    val numHashFunctions = dis.readInt
    val numWords = dis.readInt
    val data = new Array[Long](numWords)
    for (i <- 0 until numWords) {
      data(i) = dis.readLong
    }
    in.close()

    // serialize
    writtenCount = 0
    out = new ByteArrayOutputStream(byteArrayLength)
    writeIntLittleEndian(version)
    writeIntLittleEndian(numHashFunctions)
    writeIntLittleEndian(numWords)
    for (datum <- data) {
      writeLongLittleEndian(datum)
    }
    assert(writtenCount == byteArrayLength, s"Expects ${byteArrayLength} bytes, but got ${writtenCount} bytes; something went wrong in deserialize/serialize")

    // copy to off heap, init input IntVec
    val intVec: Vec = new IntVec(intVecSize)
    val byteArray = out.asInstanceOf[ByteArrayOutputStream].toByteArray
    intVec.setValuesBuf(byteArray)
    out.close()
    val inputVecs = new Array[Vec](1)
    inputVecs(0) = intVec
    new VecBatch(inputVecs, intVecSize)
  }

  private def writeIntLittleEndian(v: Int): Unit = {
    out.write((v >>> 0) & 0xFF)
    out.write((v >>> 8) & 0xFF)
    out.write((v >>> 16) & 0xFF)
    out.write((v >>> 24) & 0xFF)
    incCount(4)
  }

  private def writeLongLittleEndian(v: Long): Unit = {
    writeBuffer(0) = (v >>> 0).toByte
    writeBuffer(1) = (v >>> 8).toByte
    writeBuffer(2) = (v >>> 16).toByte
    writeBuffer(3) = (v >>> 24).toByte
    writeBuffer(4) = (v >>> 32).toByte
    writeBuffer(5) = (v >>> 40).toByte
    writeBuffer(6) = (v >>> 48).toByte
    writeBuffer(7) = (v >>> 56).toByte
    out.write(writeBuffer, 0, 8)
    incCount(8)
  }

  private def incCount(value: Int): Unit = {
    var temp = writtenCount + value
    if (temp < 0) {
      temp = Integer.MAX_VALUE
    }
    writtenCount = temp
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    ColumnarBloomFilterSubquery(scalarSubquery.plan, scalarSubquery.exprId, scalarSubquery)
}
