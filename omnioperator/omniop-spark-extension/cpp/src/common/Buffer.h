/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

 #ifndef CPP_BUFFER_H
 #define CPP_BUFFER_H

 #include <cstring>
 #include <vector>
 #include <chrono>
 #include <memory>
 #include <list>
 
 class Buffer {
 public:
        Buffer(uint8_t* data, int64_t size, int64_t capacity)
                : data_(data),
                size_(size),
                capacity_(capacity) {
        }

 public:
        uint8_t * data_;
        int64_t size_;
        int64_t capacity_;
 };

 #endif //CPP_BUFFER_H