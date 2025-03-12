/*
 * Copyright (C) 2025-2025. Huawei Technologies Co., Ltd. All rights reserved.
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

package org.apache.spark.sql.catalyst.catalog

import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId, Expression, LessThan, Literal}

import java.util
import java.util.Objects
import java.util.concurrent.{Callable, TimeUnit}
import scala.collection.mutable

case class CachedPrunedPartitionKey(qualifiedTableName: QualifiedTableName, predicates: Seq[Expression]) {

  private var hashComputed = false
  private var hash = 0
  override def hashCode(): Int = {
    if(hashComputed) {
      return hash
    }
    var result = 1

    for (element <- predicates) {
      result = 31 * result + (if (element == null) 0 else element.hashCode)
    }

    result = 31 * result + (if (qualifiedTableName == null) 0 else qualifiedTableName.hashCode)

    hash = result
    hashComputed = true
    hash
  }

  override def equals(obj: Any): Boolean = {
    if(obj == null) {
      return false
    }
    if(!obj.isInstanceOf[CachedPrunedPartitionKey]) {
      return false
    }
    val other = obj.asInstanceOf[CachedPrunedPartitionKey]
    this.qualifiedTableName == other.qualifiedTableName && this.predicates.size == other.predicates.size && this.predicates.zip(other.predicates).forall((e1) => e1._1 == e1._2)
  }
}

class CatalogTableCache(cacheSize: Int, expire: Int) extends Logging {

  private var tableRelationCache: Cache[QualifiedTableName, CatalogTable] = _

  private var prunedPartitionsCache:
    Cache[CachedPrunedPartitionKey,
      Seq[CatalogTablePartition]] = _

  initCache(cacheSize, expire)

  private def initCache(cacheSize: Int, expire: Int): Unit = {
    if(cacheSize <= 0) {
      return
    }
    tableRelationCache =
      CacheBuilder.newBuilder()
        .maximumSize(cacheSize)
        .build[QualifiedTableName, CatalogTable]()
    prunedPartitionsCache = CacheBuilder.newBuilder()
      .maximumSize(cacheSize)
      .expireAfterAccess(expire, TimeUnit.SECONDS)
      .build[CachedPrunedPartitionKey, Seq[CatalogTablePartition]]()
  }

  def getCachedTable(db: String, table: String): CatalogTable = {
    if (cacheSize <= 0) {
      return null
    }
    tableRelationCache.getIfPresent(QualifiedTableName(db, table))
  }

  private val dummyExprId = ExprId(0, null)

  def cacheTable(db: String, table: String, value: CatalogTable): Unit = {
    if (cacheSize <= 0) {
      return
    }
    tableRelationCache.put(QualifiedTableName(db, table), value)
  }

  def getCachedPartitions(
    db: String,
    table: String,
    predicate: Seq[Expression],
    callable: Callable[Seq[CatalogTablePartition]]): Seq[CatalogTablePartition] = {
    if (cacheSize <= 0) {
      return callable.call()
    }
    val newPredicates = predicate.map(e => e.transform {
      case a: AttributeReference =>
        AttributeReference(a.name, a.dataType.asNullable)(exprId = dummyExprId)
    })
    val key = CachedPrunedPartitionKey(QualifiedTableName(db, table), newPredicates)
    prunedPartitionsCache.get(key, callable)
  }

  def invalidateCachedTable(key: QualifiedTableName): Unit = {
    if (cacheSize <= 0) {
      return
    }
    tableRelationCache.invalidate(key)
  }

  /** This method provides a way to invalidate all the cached CatalogTable. */
  def invalidateAllCachedTables(): Unit = {
    if (cacheSize <= 0) {
      return
    }
    tableRelationCache.invalidateAll()
  }


  /** This method provides a way to invalidate a cached plan. */
  def invalidateCachedPartition(
    db: String,
    table: String): Unit = {
    if (cacheSize <= 0) {
      return
    }
    prunedPartitionsCache.invalidate(QualifiedTableName(db, table))
  }


  /** This method provides a way to invalidate all the cached plans. */
  def invalidateAllCachedPartition(): Unit = {
    if (cacheSize <= 0) {
      return
    }
    prunedPartitionsCache.invalidateAll()
  }

}
