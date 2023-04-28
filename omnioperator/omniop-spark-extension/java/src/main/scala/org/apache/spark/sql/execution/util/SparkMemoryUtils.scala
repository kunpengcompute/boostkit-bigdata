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

package org.apache.spark.sql.execution.util

import nova.hetu.omniruntime.memory
import nova.hetu.omniruntime.memory.MemoryManager
import org.apache.spark.{SparkEnv, TaskContext}

object SparkMemoryUtils {

  private val max: Long = SparkEnv.get.conf.getSizeAsBytes("spark.memory.offHeap.size", "1g")
  MemoryManager.setGlobalMemoryLimit(max)

  def init(): Unit = {}

  private def getLocalTaskContext: TaskContext = TaskContext.get()

  private def inSparkTask(): Boolean = {
    getLocalTaskContext != null
  }

  def addLeakSafeTaskCompletionListener[U](f: TaskContext => U): TaskContext = {
    if (!inSparkTask()) {
      throw new IllegalStateException("Not in a Spark task")
    }
    getLocalTaskContext.addTaskCompletionListener(f)
  }

}
