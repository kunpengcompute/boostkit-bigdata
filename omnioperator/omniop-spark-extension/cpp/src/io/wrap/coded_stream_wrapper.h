/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description
 */

#ifndef CODED_STREAM_WRAPPER_HH
#define CODED_STREAM_WRAPPER_HH

#include "io/Adaptor.hh"


#ifndef __clang__
  DIAGNOSTIC_IGNORE("-Wshorten-64-to-32")
  DIAGNOSTIC_IGNORE("-Wreserved-id-macro")
#endif

#if define(__GNUC__) || defined(__clang__)
  DIAGNOSTIC_IGNORE("-Wconversion")
#endif

#include <google/protobuf/io/coded_stream.h>

#endif
