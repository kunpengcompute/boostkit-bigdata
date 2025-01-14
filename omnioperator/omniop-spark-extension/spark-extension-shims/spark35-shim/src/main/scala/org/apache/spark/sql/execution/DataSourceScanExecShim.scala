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

import org.apache.spark.sql.catalyst.{FileSourceOptions, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, FileSourceConstantMetadataAttribute, FileSourceMetadataAttribute, PlanExpression}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.{BucketingUtils, DataSourceStrategy, DataSourceUtils, FilePartition, FileScanRDD, FileStatusWithMetadata, HadoopFsRelation, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

abstract class DataSourceScanExecShim(
                                       @transient relation: HadoopFsRelation,
                                       requiredSchema: StructType,
                                       dataFilters: Seq[Expression]) extends DataSourceScanExec {

  lazy val metadataColumns: Seq[AttributeReference] =
    output.collect { case FileSourceMetadataAttribute(attr) => attr }

  lazy val fileConstantMetadataColumns: Seq[AttributeReference] = output.collect {
    // Collect metadata columns to be handled outside of the scan by appending constant columns.
    case FileSourceConstantMetadataAttribute(attr) => attr
  }

  override def vectorTypes: Option[Seq[String]] =
    relation.fileFormat.vectorTypes(
      requiredSchema = requiredSchema,
      partitionSchema = relation.partitionSchema,
      relation.sparkSession.sessionState.conf).map { vectorTypes =>
      // for column-based file format, append metadata column's vector type classes if any
      vectorTypes ++ fileConstantMetadataColumns.map { _ => classOf[ConstantColumnVector].getName }
    }

  protected def isDynamicPruningFilter(e: Expression): Boolean =
    e.exists(_.isInstanceOf[PlanExpression[_]])

  @transient
  protected lazy val pushedDownFilters: Seq[Filter] = {
    val supportNestedPredicatePushdown = DataSourceUtils.supportNestedPredicatePushdown(relation)
    // `dataFilters` should not include any metadata col filters
    // because the metadata struct has been flatted in FileSourceStrategy
    // and thus metadata col filters are invalid to be pushed down
    dataFilters.filterNot(_.references.exists {
      case FileSourceMetadataAttribute(_) => true
      case _ => false
    }).flatMap(DataSourceStrategy.translateFilter(_, supportNestedPredicatePushdown))
  }

  protected def createFileScanRDD(fsRelation: HadoopFsRelation, readFile: (PartitionedFile) => Iterator[InternalRow],
                        filePartitions: Seq[FilePartition]): FileScanRDD = {
    new FileScanRDD(fsRelation.sparkSession, readFile, filePartitions,
      new StructType(requiredSchema.fields ++ fsRelation.partitionSchema.fields),
      fileConstantMetadataColumns, fsRelation.fileFormat.fileConstantMetadataExtractors,
      new FileSourceOptions(CaseInsensitiveMap(fsRelation.options)))
  }

  protected def buildFilesGroupedToBuckets(selectedPartitions: Array[PartitionDirectory]): Map[Int, Array[PartitionedFile]] = {
    selectedPartitions.flatMap { p =>
      p.files.map(f => PartitionedFileUtil.getPartitionedFile(f, p.values))
    }.groupBy { f =>
      BucketingUtils
        .getBucketId(f.toPath.getName)
        .getOrElse(throw QueryExecutionErrors.invalidBucketFile(f.urlEncodedPath))
    }
  }

  protected def buildSplitFiles(file: FileStatusWithMetadata, isSplitable: Boolean,
                                maxSplitBytes: Long, partitionValues: InternalRow): Seq[PartitionedFile] = {
    PartitionedFileUtil.splitFiles(
      sparkSession = relation.sparkSession,
      file = file,
      isSplitable = isSplitable,
      maxSplitBytes = maxSplitBytes,
      partitionValues = partitionValues
    )
  }
}
