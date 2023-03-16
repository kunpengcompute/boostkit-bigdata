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

package com.huawei.boostkit.spark.util.lock

import java.io.{FileNotFoundException, IOException}

import org.apache.hadoop.fs.{FileSystem, Path}


case class FileLock(fs: FileSystem, lockFile: Path) {
  def isLocked: Boolean = {
    if (fs.exists(lockFile)) {
      return true
    }
    false
  }

  def lock: Boolean = {
    var res = true
    try {
      val out = fs.create(lockFile, false)
      out.close()
    } catch {
      case _ =>
        res = false
    }
    res
  }

  def unLock: Boolean = {
    try {
      fs.delete(lockFile, true)
    } catch {
      case _ =>
        throw new IOException("[OmniCacheAtomic] unlock failed.")
    }
  }

  /**
   * Determine whether the lock times out.
   * The default timeout period is 1 minute.
   */
  def isTimeout: Boolean = {
    val curTime = System.currentTimeMillis()
    var modifyTime = curTime
    try {
      modifyTime = fs.getFileStatus(lockFile).getModificationTime
    } catch {
      case e: FileNotFoundException =>
      // It is not an atomic operation, so it is normal for this exception to exist.
    }
    val duration = curTime - modifyTime
    // 60000 sec equal 1 minute
    val threshold = 60000
    if (threshold < duration) {
      return true
    }
    false
  }

  /**
   * When a timeout occurs, other tasks try to release the lock.
   */
  def releaseLock(): Unit = {
    try {
      fs.delete(lockFile, true)
    } catch {
      case _: Throwable =>
    }
  }
}
