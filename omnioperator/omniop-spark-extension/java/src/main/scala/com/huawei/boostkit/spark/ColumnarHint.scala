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

package com.huawei.boostkit.spark

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan

trait TransformHint {
  val stacktrace: Option[String] =
    if (TransformHints.DEBUG) {
      Some(ExceptionUtils.getStackTrace(new Throwable()))
    } else None
}

case class TRANSFORM_SUPPORTED() extends TransformHint
case class TRANSFORM_UNSUPPORTED() extends TransformHint

object TransformHints {
  val TAG: TreeNodeTag[TransformHint] =
    TreeNodeTag[TransformHint]("omni.transformhint")

  val DEBUG = false

  def isAlreadyTagged(plan: SparkPlan): Boolean = {
    plan.getTagValue(TAG).isDefined
  }

  def isTransformable(plan: SparkPlan): Boolean = {
    if (plan.getTagValue(TAG).isDefined) {
      return plan.getTagValue(TAG).get.isInstanceOf[TRANSFORM_SUPPORTED]
    }
    false
  }

  def isNotTransformable(plan: SparkPlan): Boolean = {
    if (plan.getTagValue(TAG).isDefined) {
      return plan.getTagValue(TAG).get.isInstanceOf[TRANSFORM_UNSUPPORTED]
    }
    false
  }

  def tag(plan: SparkPlan, hint: TransformHint): Unit = {
    if (isAlreadyTagged(plan)) {
      if (isNotTransformable(plan) && hint.isInstanceOf[TRANSFORM_SUPPORTED]) {
        throw new UnsupportedOperationException(
          "Plan was already tagged as non-transformable, " +
            s"cannot mark it as transformable after that:\n${plan.toString()}")
      }
    }
    plan.setTagValue(TAG, hint)
  }

  def untag(plan: SparkPlan): Unit = {
    plan.unsetTagValue(TAG)
  }

  def tagTransformable(plan: SparkPlan): Unit = {
    tag(plan, TRANSFORM_SUPPORTED())
  }

  def tagNotTransformable(plan: SparkPlan): Unit = {
    tag(plan, TRANSFORM_UNSUPPORTED())
  }

  def getHint(plan: SparkPlan): TransformHint = {
    if (!isAlreadyTagged(plan)) {
      throw new IllegalStateException("Transform hint tag not set in plan: " + plan.toString())
    }
    plan.getTagValue(TAG).getOrElse(throw new IllegalStateException())
  }

  def getHintOption(plan: SparkPlan): Option[TransformHint] = {
    plan.getTagValue(TAG)
  }
}
