/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description
 */

#ifndef ZERO_COPY_STREAM_WRAPPER_HH
#define ZERO_COPY_STREAM_WRAPPER_HH

#include "../Adaptor.hh"

#if defined(__GNUC__) || defined(__clang__)
  DIAGNOSTIC_IGNORE("-Wdeprecated")
  DIAGNOSTIC_IGNORE("-Wpadded")
  DIAGNOSTIC_IGNORE("-Wunused-parameter")
#endif

#ifndef __clang__
  DIAGNOSTIC_IGNORE("-Wreserved-id-macro")
#endif

#include <google/protobuf/io/zero_copy_stream.h>

#endif
