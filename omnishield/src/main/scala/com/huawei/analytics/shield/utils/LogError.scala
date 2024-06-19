/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.analytics.shield.utils

import org.apache.spark.internal.Logging

object LogError extends Logging{

  def invalidArgumentError(errmsg: String, condition: Boolean = true): Unit = {
    if (condition) {
      logError(errmsg)
      throw new IllegalArgumentException(errmsg)
    }
  }

  def invalidOperationError(condition: Boolean, errmsg: String, cause: Throwable = null): Unit = {
    if (condition) {
      logError(errmsg)
      throw new InvalidOperationException(errmsg, cause)
    }
  }
}

class InvalidOperationException(message: String, cause: Throwable = null)
  extends Exception(message, cause) {
  def this(message: String) = this(message, null)
}