/**
 * Copyright (C) 2022-2023. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SPARK_THESTRAL_PLUGIN_IO_EXCEPTION_H
#define SPARK_THESTRAL_PLUGIN_IO_EXCEPTION_H

#include "stdexcept"

namespace fs {

class IOException : public std::runtime_error {
public:
    explicit IOException(const std::string &arg);

    explicit IOException(const char *arg);

    virtual ~IOException() noexcept;

    IOException(const IOException &);

private:
    IOException &operator=(const IOException &);
};

}


#endif //SPARK_THESTRAL_PLUGIN_IO_EXCEPTION_H
