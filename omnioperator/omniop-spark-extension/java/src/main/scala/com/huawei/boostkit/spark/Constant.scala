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
  val IS_SKIP_VERIFY_EXP: Boolean = true
  val OMNI_DECIMAL64_TYPE: String = DataTypeId.OMNI_DECIMAL64.ordinal().toString
  val OMNI_DECIMAL128_TYPE: String = DataTypeId.OMNI_DECIMAL128.ordinal().toString
}