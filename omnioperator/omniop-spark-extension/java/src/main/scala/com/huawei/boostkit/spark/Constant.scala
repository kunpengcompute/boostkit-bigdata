/*
 * Copyright (C) 2022-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

import nova.hetu.omniruntime.`type`.DataType.DataTypeId

/**
 * @since 2022/4/15
 */
object Constant {
  val DEFAULT_STRING_TYPE_LENGTH = 2000
  val OMNI_VARCHAR_TYPE: String = DataTypeId.OMNI_VARCHAR.ordinal().toString
  val OMNI_SHOR_TYPE: String = DataTypeId.OMNI_SHORT.ordinal().toString
  val OMNI_INTEGER_TYPE: String = DataTypeId.OMNI_INT.ordinal().toString
  val OMNI_LONG_TYPE: String = DataTypeId.OMNI_LONG.ordinal().toString
  val OMNI_DOUBLE_TYPE: String = DataTypeId.OMNI_DOUBLE.ordinal().toString
  val OMNI_BOOLEAN_TYPE: String = DataTypeId.OMNI_BOOLEAN.ordinal().toString
  val OMNI_DATE_TYPE: String = DataTypeId.OMNI_DATE32.ordinal().toString
  val IS_ENABLE_JIT: Boolean = ColumnarPluginConfig.getSessionConf.enableJit
  val IS_DECIMAL_CHECK: Boolean = ColumnarPluginConfig.getSessionConf.enableDecimalCheck
  val IS_SKIP_VERIFY_EXP: Boolean = true
  val OMNI_DECIMAL64_TYPE: String = DataTypeId.OMNI_DECIMAL64.ordinal().toString
  val OMNI_DECIMAL128_TYPE: String = DataTypeId.OMNI_DECIMAL128.ordinal().toString
}