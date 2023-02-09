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

package org.apache.spark.sql.catalyst.optimizer.rules

class OuterJoinSuite extends RewriteSuite {

  // Since FULL OUTER JOIN cannot push the predicate down,
  // it cannot compensate the predicate,
  // so OUTER JOIN does not include FULL OUTER JOIN.
  val OUTER_JOINS = List("LEFT JOIN", "RIGHT JOIN", "SEMI JOIN", "ANTI JOIN")

  def runOuterJoinFunc(fun: (String, Int) => Unit)(viewNumber: Int): Unit = {
    OUTER_JOINS.foreach {
      outJoin =>
        fun(outJoin, viewNumber)
    }
  }
}
