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

package org.apache.spark.sql.execution.datasources.parquet

import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor.sparkTypeToOmniType
import com.huawei.boostkit.spark.jni.{OrcColumnarBatchWriter, ParquetColumnarBatchWriter}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.{OmniInternalRow, OutputWriter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import scala.Array.{emptyBooleanArray, emptyIntArray}

// NOTE: This class is instantiated and used on executor side only, no need to be serializable.
class OmniParquetOutputWriter(path: String, dataSchema: StructType,
                              context: TaskAttemptContext)
  extends OutputWriter {
  private val datetimeRebaseMode = SQLConf.get.getConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE)

  val writer = new ParquetColumnarBatchWriter(datetimeRebaseMode == "LEGACY")
  var omniTypes: Array[Int] = emptyIntArray
  var dataColumnsIds: Array[Boolean] = emptyBooleanArray
  var allOmniTypes: Array[Int] = emptyIntArray

  def initialize(allColumns: Seq[Attribute], dataColumns: Seq[Attribute]): Unit = {
    val filePath = new Path(path)
    writer.initializeSchemaJava(dataSchema)
    writer.initializeWriterJava(filePath)
    omniTypes = dataSchema.fields
      .map(field => sparkTypeToOmniType(field.dataType, field.metadata).getId.ordinal())
      .toArray
    allOmniTypes = allColumns.toStructType.fields
      .map(field => sparkTypeToOmniType(field.dataType, field.metadata).getId.ordinal())
      .toArray
    dataColumnsIds = allColumns.map(x => dataColumns.contains(x)).toArray
  }

  override def write(row: InternalRow): Unit = {
    assert(row.isInstanceOf[OmniInternalRow])
    writer.write(omniTypes, dataColumnsIds, row.asInstanceOf[OmniInternalRow].batch)
  }

  def spiltWrite(row: InternalRow, startPos: Long, endPos: Long): Unit = {
    assert(row.isInstanceOf[OmniInternalRow])
    writer.splitWrite(omniTypes, allOmniTypes, dataColumnsIds,
      row.asInstanceOf[OmniInternalRow].batch, startPos, endPos)
  }

  override def close(): Unit = {
    writer.close()
  }

  override def path(): String = {
    path
  }
}
