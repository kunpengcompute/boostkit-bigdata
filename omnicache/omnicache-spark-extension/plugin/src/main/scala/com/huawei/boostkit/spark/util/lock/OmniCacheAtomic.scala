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

import com.huawei.boostkit.spark.util.RewriteLogger


object OmniCacheAtomic extends RewriteLogger {
  // func atomicity is guaranteed through file locks
  private def atomicFunc(fileLock: FileLock)(func: () => Unit): Boolean = {
    if (fileLock.isLocked || !fileLock.lock) {
      return false
    }
    try {
      func()
    } catch {
      case e: Throwable =>
        throw e
    } finally {
      fileLock.unLock
    }
    true
  }

  private def timeoutReleaseLock(fileLock: FileLock): Unit = {
    if (fileLock.isTimeout) {
      logError("[Omni Atomic] lock expired.")
      fileLock.releaseLock()
    }
  }

  // The spin waits or gets the lock to perform the operation
  def funcWithSpinLock(fileLock: FileLock)(func: () => Unit): Unit = {
    while (!atomicFunc(fileLock)(func)) {
      logInfo("[Omni Atomic] wait for lock.")
      timeoutReleaseLock(fileLock)
    }
  }
}
