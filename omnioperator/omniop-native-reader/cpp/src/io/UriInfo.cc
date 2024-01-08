/**
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

#include <string>
#include <utility>

#include "UriInfo.h"

UriInfo::UriInfo(std::string _uri, std::string _scheme, std::string _path, std::string _host,
                 std::string _port): host_string(std::move(_host)),
                                     scheme_string(std::move(_scheme)),
                                     port_string(std::move(_port)),
                                     path_string(std::move(_path)),
                                     uri_string(std::move(_uri))
{
}

UriInfo::UriInfo(std::string _scheme, std::string _path, std::string _host,
                 std::string _port): host_string(std::move(_host)),
                                     scheme_string(std::move(_scheme)),
                                     port_string(std::move(_port)),
                                     path_string(std::move(_path)),
                                     uri_string("Not initialize origin uri!")
{
}

UriInfo::~UriInfo() {}

const std::string UriInfo::scheme() const {
    return scheme_string;
}

const std::string UriInfo::host() const {
    return host_string;
}

const std::string UriInfo::port() const {
    return port_string;
}

const std::string UriInfo::path() const {
    return path_string;
}

const std::string UriInfo::ToString() const {
    return uri_string;
}