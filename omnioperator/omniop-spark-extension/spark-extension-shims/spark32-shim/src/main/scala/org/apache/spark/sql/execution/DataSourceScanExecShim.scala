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

package org.apache.spark.sql.execution

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, PlanExpression}
import org.apache.spark.sql.execution.datasources.{BucketingUtils, DataSourceStrategy, DataSourceUtils, FilePartition, FileScanRDD, HadoopFsRelation, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

abstract class DataSourceScanExecShim(
                                       @transient relation: HadoopFsRelation,
                                       requiredSchema: StructType,
                                       dataFilters: Seq[Expression]) extends DataSourceScanExec {

  override def vectorTypes: Option[Seq[String]] =
    relation.fileFormat.vectorTypes(
      requiredSchema = requiredSchema,
      partitionSchema = relation.partitionSchema,
      relation.sparkSession.sessionState.conf)

  protected def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined

  @transient
  protected lazy val pushedDownFilters: Seq[Filter] = {
    val supportNestedPredicatePushdown = DataSourceUtils.supportNestedPredicatePushdown(relation)
    dataFilters.flatMap(DataSourceStrategy.translateFilter(_, supportNestedPredicatePushdown))
  }

  protected def createFileScanRDD(fsRelation: HadoopFsRelation, readFile: (PartitionedFile) => Iterator[InternalRow],
                        filePartitions: Seq[FilePartition]): FileScanRDD = {
    new FileScanRDD(fsRelation.sparkSession, readFile, filePartitions)
  }

  protected def buildFilesGroupedToBuckets(selectedPartitions: Array[PartitionDirectory]): Map[Int, Array[PartitionedFile]] = {
    selectedPartitions.flatMap { p =>
      p.files.map { f =>
        PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values)
      }
    }.groupBy { f =>
      BucketingUtils
        .getBucketId(new Path(f.filePath).getName)
        .getOrElse(sys.error(s"Invalid bucket file ${f.filePath}"))
    }
  }

  protected def buildSplitFiles(file: FileStatus, isSplitable: Boolean,
                                maxSplitBytes: Long, partitionValues: InternalRow): Seq[PartitionedFile] = {
    PartitionedFileUtil.splitFiles(
      sparkSession = relation.sparkSession,
      file = file,
      filePath = file.getPath,
      isSplitable = isSplitable,
      maxSplitBytes = maxSplitBytes,
      partitionValues = partitionValues
    )
  }
}
