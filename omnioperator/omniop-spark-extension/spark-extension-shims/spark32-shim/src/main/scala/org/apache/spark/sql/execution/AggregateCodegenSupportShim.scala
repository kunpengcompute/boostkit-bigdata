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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec

trait AggregateCodegenSupportShim extends BaseAggregateExec with BlockingOperatorWithCodegen
  with GeneratePredicateHelper {

  protected def needHashTable: Boolean

  protected def doConsumeWithKeys(ctx: CodegenContext, input: Seq[ExprCode]): String

  protected def doProduceWithKeys(ctx: CodegenContext): String

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    throw new UnsupportedOperationException("ColumnarHashAgg code-gen does not support produce")
  }
}
