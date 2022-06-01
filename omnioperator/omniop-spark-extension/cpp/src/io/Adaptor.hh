/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description
 */

#ifndef ADAPTER_HH
#define ADAPTER_HH

#define PRAGMA(TXT) _Pragma(#TXT)

#ifdef __clang__
  #define DIAGNOSTIC_IGNORE(XXX) PRAGMA(clang diagnostic ignored XXX)
#elif defined(__GNUC__)
  #define DIAGNOSTIC_IGNORE(XXX) PRAGMA(GCC diagnostic ignored XXX)
#elif defined(_MSC_VER)
  #define DIAGNOSTIC_IGNORE(XXX) __pragma(warning(disable : XXX))
#else
  #define DIAGNOSTIC_IGNORE(XXX)
#endif

#endif