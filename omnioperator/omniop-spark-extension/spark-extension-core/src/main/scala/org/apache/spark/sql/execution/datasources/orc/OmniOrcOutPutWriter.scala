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

package org.apache.spark.sql.execution.datasources.orc

import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor.{sparkTypeToOmniExpType, sparkTypeToOmniType}
import com.huawei.boostkit.spark.jni.OrcColumnarBatchWriter
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{OmniInternalRow, OutputWriter}
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.orc.{OrcConf, OrcFile}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.StructType

import java.net.URI

private[sql] class OmniOrcOutputWriter(path: String, dataSchema: StructType,
                                       context: TaskAttemptContext) extends OutputWriter {

  val writer = new OrcColumnarBatchWriter()
  var omniTypes: Array[Int] = new Array[Int](0)
  var dataColumnsIds: Array[Boolean] = new Array[Boolean](0)
  var allOmniTypes: Array[Int] = new Array[Int](0)

  def initialize(allColumns: Seq[Attribute], dataColumns: Seq[Attribute]): Unit = {
    val filePath = new Path(path)
    val conf = context.getConfiguration
    val writerOptions = OrcFile.writerOptions(conf).
      fileSystem(filePath.getFileSystem(conf))
    writer.initializeOutputStreamJava(filePath.toUri)
    writer.initializeSchemaTypeJava(dataSchema)
    writer.initializeWriterJava(filePath.toUri, dataSchema, writerOptions)
    dataSchema.foreach(field => {
      omniTypes = omniTypes :+ sparkTypeToOmniType(field.dataType, field.metadata).getId.ordinal()
    })
    allColumns.toStructType.foreach(field => {
      allOmniTypes = allOmniTypes :+ sparkTypeToOmniType(field.dataType, field.metadata)
        .getId.ordinal()
    })
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
